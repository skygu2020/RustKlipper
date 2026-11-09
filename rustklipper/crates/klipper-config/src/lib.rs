//! Klipper-style configuration parsing primitives.
//!
//! This crate is the Rust counterpart of the parsing layer in
//! `klippy/configfile.py`. It intentionally preserves section order and
//! option insertion order so later config loading can reproduce Klipper's
//! deterministic traversal. File includes are handled by the loader layer.
use std::collections::{HashMap, HashSet};
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
    autosave_options: HashSet<(String, String)>,
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

    fn merge_missing(&mut self, other: ConfigDocument) {
        for incoming in other.sections {
            let section_index = if let Some(&index) = self.index.get(&incoming.name) {
                index
            } else {
                let index = self.sections.len();
                self.index.insert(incoming.name.clone(), index);
                self.sections
                    .push(ConfigSection::new(incoming.name.clone(), incoming.line));
                index
            };
            for option in incoming.options {
                if !self.sections[section_index].has_option(&option.name) {
                    self.autosave_options.insert((
                        incoming.name.to_ascii_lowercase(),
                        option.name.to_ascii_lowercase(),
                    ));
                    self.sections[section_index].set(
                        option.name,
                        option.value,
                        option.line,
                    );
                }
            }
        }
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

    pub fn is_autosave_option(&self, section: &str, option: &str) -> bool {
        self.autosave_options
            .contains(&(section.to_ascii_lowercase(), option.to_ascii_lowercase()))
    }

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

pub const AUTOSAVE_HEADER: &str =
    "#*# <---------------------- SAVE_CONFIG ---------------------->";

impl ConfigFileLoader {
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<ConfigDocument, ConfigLoadError> {
        let path = path.as_ref();
        let mut visited = std::collections::HashSet::new();
        let mut config = Self::load_inner(path, &mut visited)?;
        if let Some(autosave) = Self::load_autosave(path)? {
            config.merge_missing(autosave);
        }
        Ok(config)
    }

    fn load_autosave(
        path: &std::path::Path,
    ) -> Result<Option<ConfigDocument>, ConfigLoadError> {
        let data = std::fs::read_to_string(path).map_err(|source| ConfigLoadError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let Some(marker_index) = data.find(AUTOSAVE_HEADER) else {
            return Ok(None);
        };
        let marker_line = data[..marker_index].lines().count() + 1;
        let autosave = &data[marker_index + AUTOSAVE_HEADER.len()..];
        if autosave.contains(AUTOSAVE_HEADER) {
            return Ok(None);
        }

        let mut lines = Vec::new();
        for line in autosave.lines() {
            if line.trim().is_empty() || line == "#*#" {
                lines.push(String::new());
            } else if let Some(content) = line.strip_prefix("#*# ") {
                lines.push(content.to_owned());
            } else {
                // Match Klipper's behavior: a malformed autosave block is
                // ignored rather than allowing its contents into normal config.
                return Ok(None);
            }
        }
        if lines.iter().all(|line| line.trim().is_empty()) {
            return Ok(None);
        }
        let parsed = ConfigDocument::parse_named_at(
            &lines.join("\n"),
            format!("{} (autosave)", path.display()),
            marker_line,
        )?;
        Ok(Some(parsed))
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
    warnings: std::sync::Mutex<Vec<ConfigWarning>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnusedConfigOption {
    pub section: String,
    pub option: String,
    pub line: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigWarningKind {
    DeprecatedOption,
    DeprecatedValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigWarning {
    pub kind: ConfigWarningKind,
    pub section: String,
    pub option: String,
    pub value: Option<String>,
    pub message: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConfigValidationError {
    #[error("section '{section}' is not a valid config section")]
    UnknownSection { section: String },
    #[error("option '{option}' is not valid in section '{section}'")]
    UnknownOption { section: String, option: String },
}

/// A section view that records option reads against a usage tracker.
pub struct TrackedConfigSection<'a> {
    section: &'a ConfigSection,
    tracker: &'a ConfigUsageTracker,
}

impl<'a> TrackedConfigSection<'a> {
    pub fn name(&self) -> &'a str {
        &self.section.name
    }

    pub fn line(&self) -> usize {
        self.section.line
    }

    pub fn get(&self, option: &str) -> Option<&'a str> {
        let value = self.section.get(option)?;
        self.tracker.mark_used(&self.section.name, option);
        Some(value)
    }

    pub fn has_option(&self, option: &str) -> bool {
        self.section.has_option(option)
    }

    /// Record a deprecation warning when the option exists in this section.
    pub fn deprecate(
        &self,
        option: &str,
        value: Option<&str>,
        message: Option<&str>,
    ) {
        if self.has_option(option) {
            self.tracker
                .deprecate(&self.section.name, option, value, message);
        }
    }

    /// Read an option or return a default; a supplied default counts as used.
    pub fn get_with_default(
        &self,
        option: &str,
        default: Option<&'a str>,
        note_valid: bool,
    ) -> Option<&'a str> {
        if let Some(value) = self.section.get(option) {
            if note_valid {
                self.tracker.mark_used(&self.section.name, option);
            }
            return Some(value);
        }
        if default.is_some() && note_valid {
            self.tracker.mark_used(&self.section.name, option);
        }
        default
    }

    pub fn get_required(&self, option: &str) -> Result<&'a str, ConfigValueError> {
        self.get(option).ok_or_else(|| ConfigValueError::Missing {
            section: self.section.name.clone(),
            option: option.to_owned(),
        })
    }

    pub fn get_list(
        &self,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<String>, ConfigValueError> {
        let value = self.get_required(option)?;
        let values: Vec<String> = if value.trim().is_empty() {
            Vec::new()
        } else {
            value
                .split(separator)
                .map(str::trim)
                .map(str::to_owned)
                .collect()
        };
        if let Some(expected) = count {
            if values.len() != expected {
                return Err(ConfigValueError::ElementCount {
                    section: self.section.name.clone(),
                    option: option.to_owned(),
                    expected,
                    actual: values.len(),
                });
            }
        }
        Ok(values)
    }

    pub fn get_f64_list(
        &self,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<f64>, ConfigValueError> {
        self.get_list(option, separator, count)?
            .into_iter()
            .map(|value| {
                let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
                    section: self.section.name.clone(),
                    option: option.to_owned(),
                    value: value.clone(),
                    expected: "float list",
                })?;
                if !parsed.is_finite() {
                    return Err(ConfigValueError::Invalid {
                        section: self.section.name.clone(),
                        option: option.to_owned(),
                        value,
                        expected: "finite float list",
                    });
                }
                Ok(parsed)
            })
            .collect()
    }

    pub fn get_i64_list(
        &self,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<i64>, ConfigValueError> {
        self.get_list(option, separator, count)?
            .into_iter()
            .map(|value| {
                value.parse().map_err(|_| ConfigValueError::Invalid {
                    section: self.section.name.clone(),
                    option: option.to_owned(),
                    value,
                    expected: "integer list",
                })
            })
            .collect()
    }

    pub fn get_i64(&self, option: &str) -> Result<i64, ConfigValueError> {
        let value = self.get_required(option)?;
        value.parse().map_err(|_| ConfigValueError::Invalid {
            section: self.section.name.clone(),
            option: option.to_owned(),
            value: value.to_owned(),
            expected: "integer",
        })
    }

    pub fn get_f64(&self, option: &str) -> Result<f64, ConfigValueError> {
        let value = self.get_required(option)?;
        let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
            section: self.section.name.clone(),
            option: option.to_owned(),
            value: value.to_owned(),
            expected: "float",
        })?;
        if !parsed.is_finite() {
            return Err(ConfigValueError::Invalid {
                section: self.section.name.clone(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "finite float",
            });
        }
        Ok(parsed)
    }

    /// Read a string option and map it through an explicit set of choices.
    pub fn get_choice<T: Clone>(
        &self,
        option: &str,
        choices: &[(String, T)],
        default: Option<&'a str>,
    ) -> Result<T, ConfigValueError> {
        let value = self
            .get_with_default(option, default, true)
            .ok_or_else(|| ConfigValueError::Missing {
                section: self.section.name.clone(),
                option: option.to_owned(),
            })?;
        choices
            .iter()
            .find(|(choice, _)| choice == value)
            .map(|(_, result)| result.clone())
            .ok_or_else(|| ConfigValueError::Invalid {
                section: self.section.name.clone(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "valid choice",
            })
    }

    pub fn get_bool(&self, option: &str) -> Result<bool, ConfigValueError> {
        let value = self.get_required(option)?;
        match value.to_ascii_lowercase().as_str() {
            "1" | "yes" | "true" | "on" => Ok(true),
            "0" | "no" | "false" | "off" => Ok(false),
            _ => Err(ConfigValueError::Invalid {
                section: self.section.name.clone(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "boolean",
            }),
        }
    }

    pub fn prefixed_options(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = &'a ConfigOption> {
        let options: Vec<_> = self.section.prefixed_options(prefix).collect();
        for option in &options {
            self.tracker.mark_used(&self.section.name, &option.name);
        }
        options.into_iter()
    }
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
        self.warnings
            .lock()
            .expect("config warning lock poisoned")
            .clear();
    }

    /// Add a deduplicated deprecation warning.
    pub fn deprecate(
        &self,
        section: &str,
        option: &str,
        value: Option<&str>,
        message: Option<&str>,
    ) {
        let kind = if value.is_some() {
            ConfigWarningKind::DeprecatedValue
        } else {
            ConfigWarningKind::DeprecatedOption
        };
        let default_message = match value {
            Some(value) => format!(
                "Value '{value}' in option '{option}' in section '{section}' is deprecated."
            ),
            None => format!("Option '{option}' in section '{section}' is deprecated."),
        };
        let warning = ConfigWarning {
            kind,
            section: section.to_owned(),
            option: option.to_owned(),
            value: value.map(str::to_owned),
            message: message.unwrap_or(&default_message).to_owned(),
        };
        let mut warnings = self.warnings.lock().expect("config warning lock poisoned");
        if !warnings.contains(&warning) {
            warnings.push(warning);
        }
    }

    pub fn warnings(&self) -> Vec<ConfigWarning> {
        self.warnings
            .lock()
            .expect("config warning lock poisoned")
            .clone()
    }

    pub fn track_section<'a>(
        &'a self,
        section: &'a ConfigSection,
    ) -> TrackedConfigSection<'a> {
        TrackedConfigSection {
            section,
            tracker: self,
        }
    }

    fn mark_used(&self, section: &str, option: &str) {
        self.used
            .lock()
            .expect("config usage lock poisoned")
            .insert((section.to_ascii_lowercase(), option.to_ascii_lowercase()));
    }

    /// Read an option and mark it as used when it exists.
    pub fn get<'a>(
        &self,
        config: &'a ConfigDocument,
        section: &str,
        option: &str,
    ) -> Option<&'a str> {
        let value = config.get(section, option)?;
        self.mark_used(section, option);
        Some(value)
    }

    /// Read an option or return a default; a supplied default counts as used.
    pub fn get_with_default<'a>(
        &self,
        config: &'a ConfigDocument,
        section: &str,
        option: &str,
        default: Option<&'a str>,
        note_valid: bool,
    ) -> Option<&'a str> {
        if let Some(value) = config.get(section, option) {
            if note_valid {
                self.mark_used(section, option);
            }
            return Some(value);
        }
        if default.is_some() && note_valid {
            self.mark_used(section, option);
        }
        default
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

    pub fn get_list(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<String>, ConfigValueError> {
        let value = self.get_required(config, section, option)?;
        let values: Vec<String> = if value.trim().is_empty() {
            Vec::new()
        } else {
            value
                .split(separator)
                .map(str::trim)
                .map(str::to_owned)
                .collect()
        };
        if let Some(expected) = count {
            if values.len() != expected {
                return Err(ConfigValueError::ElementCount {
                    section: section.to_owned(),
                    option: option.to_owned(),
                    expected,
                    actual: values.len(),
                });
            }
        }
        Ok(values)
    }

    pub fn get_i64_list(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<i64>, ConfigValueError> {
        self.get_list(config, section, option, separator, count)?
            .into_iter()
            .map(|value| {
                value.parse().map_err(|_| ConfigValueError::Invalid {
                    section: section.to_owned(),
                    option: option.to_owned(),
                    value,
                    expected: "integer list",
                })
            })
            .collect()
    }

    pub fn get_f64_list(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
        separator: char,
        count: Option<usize>,
    ) -> Result<Vec<f64>, ConfigValueError> {
        self.get_list(config, section, option, separator, count)?
            .into_iter()
            .map(|value| {
                let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
                    section: section.to_owned(),
                    option: option.to_owned(),
                    value: value.clone(),
                    expected: "float list",
                })?;
                if !parsed.is_finite() {
                    return Err(ConfigValueError::Invalid {
                        section: section.to_owned(),
                        option: option.to_owned(),
                        value,
                        expected: "finite float list",
                    });
                }
                Ok(parsed)
            })
            .collect()
    }

    pub fn get_choice<T: Clone>(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
        choices: &[(String, T)],
        default: Option<&str>,
    ) -> Result<T, ConfigValueError> {
        let value = self
            .get_with_default(config, section, option, default, true)
            .ok_or_else(|| ConfigValueError::Missing {
                section: section.to_owned(),
                option: option.to_owned(),
            })?;
        choices
            .iter()
            .find(|(choice, _)| choice == value)
            .map(|(_, result)| result.clone())
            .ok_or_else(|| ConfigValueError::Invalid {
                section: section.to_owned(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "valid choice",
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

    pub fn get_f64(
        &self,
        config: &ConfigDocument,
        section: &str,
        option: &str,
    ) -> Result<f64, ConfigValueError> {
        let value = self.get_required(config, section, option)?;
        let parsed: f64 = value.parse().map_err(|_| ConfigValueError::Invalid {
            section: section.to_owned(),
            option: option.to_owned(),
            value: value.to_owned(),
            expected: "float",
        })?;
        if !parsed.is_finite() {
            return Err(ConfigValueError::Invalid {
                section: section.to_owned(),
                option: option.to_owned(),
                value: value.to_owned(),
                expected: "finite float",
            });
        }
        Ok(parsed)
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

    /// Validate section and option names against registered objects and reads.
    pub fn validate(
        &self,
        config: &ConfigDocument,
        valid_sections: &[String],
    ) -> Result<(), ConfigValidationError> {
        let mut valid: std::collections::HashSet<String> = valid_sections
            .iter()
            .map(|section| section.to_ascii_lowercase())
            .collect();
        let used = self.used.lock().expect("config usage lock poisoned");
        valid.extend(used.iter().map(|(section, _)| section.clone()));
        valid.extend(
            config
                .autosave_options
                .iter()
                .map(|(section, _)| section.clone()),
        );

        for section in config.sections() {
            let section_name = section.name.to_ascii_lowercase();
            if !valid.contains(&section_name) {
                return Err(ConfigValidationError::UnknownSection {
                    section: section.name.clone(),
                });
            }
            for option in section.options() {
                let key = (section_name.clone(), option.name.clone());
                if config.is_autosave_option(&section.name, &option.name) {
                    continue;
                }
                if !used.contains(&key) {
                    return Err(ConfigValidationError::UnknownOption {
                        section: section.name.clone(),
                        option: option.name.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Return every option that no component has read, preserving config order.
    pub fn unused_options(&self, config: &ConfigDocument) -> Vec<UnusedConfigOption> {
        let used = self.used.lock().expect("config usage lock poisoned");
        config
            .sections()
            .iter()
            .flat_map(|section| {
                section.options().iter().filter_map(|option| {
                    let key = (section.name.to_ascii_lowercase(), option.name.clone());
                    (!config.is_autosave_option(&section.name, &option.name)
                        && !used.contains(&key))
                    .then(|| UnusedConfigOption {
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
    fn usage_tracker_validates_unknown_sections_and_options() {
        let config = ConfigDocument::parse(
            "[printer]\nkinematics=cartesian\nunknown_option=1\n[unknown]\nvalue=2\n",
        )
        .unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(tracker.get(&config, "printer", "kinematics"), Some("cartesian"));
        let valid_sections = vec!["printer".to_owned()];
        assert_eq!(
            tracker.validate(&config, &valid_sections),
            Err(ConfigValidationError::UnknownOption {
                section: "printer".to_owned(),
                option: "unknown_option".to_owned(),
            })
        );

        let clean = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        tracker.clear();
        tracker.get(&clean, "printer", "kinematics");
        assert!(tracker.validate(&clean, &valid_sections).is_ok());
    }

    #[test]
    fn deprecation_warnings_are_deduplicated_and_reset() {
        let config = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("printer").unwrap());
        section.deprecate("kinematics", None, None);
        section.deprecate("kinematics", None, None);
        assert_eq!(tracker.warnings().len(), 1);
        assert_eq!(
            tracker.warnings()[0].kind,
            ConfigWarningKind::DeprecatedOption
        );
        section.deprecate("kinematics", Some("cartesian"), Some("Use a new value"));
        assert_eq!(tracker.warnings().len(), 2);
        assert_eq!(
            tracker.warnings()[1].kind,
            ConfigWarningKind::DeprecatedValue
        );
        assert_eq!(tracker.warnings()[1].message, "Use a new value");
        tracker.clear();
        assert!(tracker.warnings().is_empty());
    }

    #[test]
    fn config_defaults_are_tracked_only_when_non_null_and_valid() {
        let config = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(tracker.get(&config, "printer", "kinematics"), Some("cartesian"));
        assert_eq!(
            tracker.get_with_default(&config, "printer", "missing", Some("default"), true),
            Some("default")
        );
        assert_eq!(
            tracker.get_with_default(&config, "printer", "optional", None, true),
            None
        );
        assert!(tracker.unused_options(&config).is_empty());
    }

    #[test]
    fn missing_deprecated_options_do_not_emit_warnings() {
        let config = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("printer").unwrap());
        section.deprecate("missing_option", None, None);
        assert!(tracker.warnings().is_empty());
    }

    #[test]
    fn config_reads_can_skip_access_tracking() {
        let config = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(
            tracker.get_with_default(
                &config,
                "printer",
                "kinematics",
                Some("fallback"),
                false,
            ),
            Some("cartesian")
        );
        assert_eq!(tracker.unused_options(&config).len(), 1);
    }

    #[test]
    fn usage_tracker_can_be_reset_for_config_reload() {
        let config = ConfigDocument::parse("[printer]\nkinematics=cartesian\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(tracker.get(&config, "printer", "kinematics"), Some("cartesian"));
        assert!(tracker.unused_options(&config).is_empty());
        tracker.clear();
        assert_eq!(tracker.unused_options(&config).len(), 1);
    }

    #[test]
    fn tracked_section_supports_typed_and_prefixed_reads() {
        let config = ConfigDocument::parse(
            "[heater]\nmax_temp=275\ncontrol=pid\npid_kp=22.2\npid_ki=1.1\n",
        )
        .unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("heater").unwrap());
        assert_eq!(section.get_i64("max_temp").unwrap(), 275);
        assert_eq!(
            section.prefixed_options("pid_").count(),
            2
        );
        let unused = tracker.unused_options(&config);
        assert_eq!(unused.len(), 1);
        assert_eq!(unused[0].option, "control");
    }

    #[test]
    fn tracked_section_parses_finite_float_lists() {
        let config = ConfigDocument::parse("[bed_mesh]\nmesh_min=0.5, 1.25\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("bed_mesh").unwrap());
        assert_eq!(
            section.get_f64_list("mesh_min", ',', Some(2)).unwrap(),
            vec![0.5, 1.25]
        );
    }

    #[test]
    fn tracked_section_maps_choice_values() {
        let config = ConfigDocument::parse("[heater]\ncontrol=pid\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("heater").unwrap());
        let choices = vec![
            ("pid".to_owned(), 1u8),
            ("watermark".to_owned(), 2u8),
        ];
        assert_eq!(section.get_choice("control", &choices, None).unwrap(), 1);
        assert!(tracker.unused_options(&config).is_empty());
        assert!(matches!(
            section.get_choice("control", &choices[..1], None),
            Err(ConfigValueError::Invalid { .. })
        ));
    }

    #[test]
    fn tracked_section_parses_lists_and_checks_element_count() {
        let config = ConfigDocument::parse("[stepper_x]\nposition=10, 20,30\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("stepper_x").unwrap());
        assert_eq!(
            section.get_i64_list("position", ',', Some(3)).unwrap(),
            vec![10, 20, 30]
        );
        assert!(matches!(
            section.get_list("position", ',', Some(2)),
            Err(ConfigValueError::ElementCount {
                expected: 2,
                actual: 3,
                ..
            })
        ));
    }

    #[test]
    fn tracked_section_parses_finite_float_values() {
        let config = ConfigDocument::parse("[heater]\nmax_power=0.75\n").unwrap();
        let tracker = ConfigUsageTracker::new();
        let section = tracker.track_section(config.section("heater").unwrap());
        assert_eq!(section.get_f64("max_power").unwrap(), 0.75);
        assert!(tracker.unused_options(&config).is_empty());
    }

    #[test]
    fn usage_tracker_parses_integer_and_float_lists() {
        let config = ConfigDocument::parse(
            "[stepper_x]\nposition=10, 20,30\n[bed_mesh]\nmesh_min=0.5,1.25\n",
        )
        .unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(
            tracker
                .get_i64_list(&config, "stepper_x", "position", ',', Some(3))
                .unwrap(),
            vec![10, 20, 30]
        );
        assert_eq!(
            tracker
                .get_f64_list(&config, "bed_mesh", "mesh_min", ',', Some(2))
                .unwrap(),
            vec![0.5, 1.25]
        );
        assert!(tracker.unused_options(&config).is_empty());
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
    fn regular_config_values_are_not_marked_as_autosave() {
        use std::fs;
        let path = std::env::temp_dir().join(format!(
            "rustklipper-config-no-autosave-{}.cfg",
            std::process::id()
        ));
        fs::write(&path, "[printer]\nkinematics=cartesian\n").unwrap();
        let config = ConfigFileLoader::load(&path).unwrap();
        assert!(!config.is_autosave_option("printer", "kinematics"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn loads_autosave_values_without_overriding_main_config() {
        use std::fs;
        let path = std::env::temp_dir().join(format!(
            "rustklipper-config-autosave-{}.cfg",
            std::process::id()
        ));
        fs::write(
            &path,
            "[printer]\nkinematics=cartesian\n#*# <---------------------- SAVE_CONFIG ---------------------->\n#*# [printer]\n#*# kinematics = delta\n#*# max_velocity = 250\n#*#\n#*# [stepper_x]\n#*# position_endstop = 0\n",
        )
        .unwrap();
        let config = ConfigFileLoader::load(&path).unwrap();
        assert_eq!(config.get("printer", "kinematics"), Some("cartesian"));
        assert_eq!(config.get("printer", "max_velocity"), Some("250"));
        assert_eq!(config.get("stepper_x", "position_endstop"), Some("0"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn autosave_option_line_numbers_match_source_file() {
        use std::fs;
        let path = std::env::temp_dir().join(format!(
            "rustklipper-config-autosave-lines-{}.cfg",
            std::process::id()
        ));
        fs::write(
            &path,
            "[printer]\nkinematics=cartesian\n#*# <---------------------- SAVE_CONFIG ---------------------->\n#*# [printer]\n#*# max_velocity = 250\n",
        )
        .unwrap();
        let config = ConfigFileLoader::load(&path).unwrap();
        let option = config
            .section("printer")
            .unwrap()
            .options()
            .iter()
            .find(|option| option.name == "max_velocity")
            .unwrap();
        assert_eq!(option.line, 5);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn autosave_options_are_exempt_from_unused_option_validation() {
        use std::fs;
        let path = std::env::temp_dir().join(format!(
            "rustklipper-config-autosave-validation-{}.cfg",
            std::process::id()
        ));
        fs::write(
            &path,
            "[printer]\nkinematics=cartesian\n#*# <---------------------- SAVE_CONFIG ---------------------->\n#*# [bed_mesh]\n#*# profile = default\n",
        )
        .unwrap();
        let config = ConfigFileLoader::load(&path).unwrap();
        let tracker = ConfigUsageTracker::new();
        assert_eq!(
            tracker.get(&config, "printer", "kinematics"),
            Some("cartesian")
        );
        assert!(tracker.unused_options(&config).is_empty());
        assert!(tracker.validate(&config, &["printer".to_owned()]).is_ok());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ignores_corrupted_autosave_blocks() {
        use std::fs;
        let path = std::env::temp_dir().join(format!(
            "rustklipper-config-autosave-corrupt-{}.cfg",
            std::process::id()
        ));
        fs::write(
            &path,
            "[printer]\nkinematics=cartesian\n#*# <---------------------- SAVE_CONFIG ---------------------->\n#*# [printer]\n#*# <---------------------- SAVE_CONFIG ---------------------->\n",
        )
        .unwrap();
        let config = ConfigFileLoader::load(&path).unwrap();
        assert_eq!(config.get("printer", "kinematics"), Some("cartesian"));
        assert!(!config.section("printer").unwrap().has_option("max_velocity"));
        fs::remove_file(path).unwrap();
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
