use super::Theme;
use crate::cli::{Cli, InheritedOutputMode, OutputFormat};
use rich_rust::prelude::*;
use rich_rust::renderables::Renderable;
use serde::Serialize;
use std::io::{self, IsTerminal, Write};
use std::sync::OnceLock;
use toon_rust::options::KeyFoldingMode;
use toon_rust::{EncodeOptions, JsonValue, encode};

/// Central output coordinator that respects robot/json/quiet modes.
///
/// Uses lazy initialization for console and theme to ensure zero overhead
/// in JSON/Quiet modes where rich output is never used.
pub struct OutputContext {
    /// Output mode (always set eagerly - cheap)
    mode: OutputMode,
    /// Terminal width (cached, lazy)
    width: OnceLock<usize>,
    /// Rich console for human-readable output (lazy)
    console: OnceLock<Console>,
    /// Theme for consistent styling (lazy)
    theme: OnceLock<Theme>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Full rich formatting (tables, colors, panels)
    Rich,
    /// Plain text, no ANSI codes (for piping)
    Plain,
    /// JSON output only
    Json,
    /// TOON format (token-optimized object notation)
    Toon,
    /// Minimal output (quiet mode)
    Quiet,
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl CountingWriter {
    const fn len(&self) -> usize {
        self.bytes
    }
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl OutputContext {
    /// Create from CLI global args.
    ///
    /// Only mode is set eagerly; console/theme/width are lazy-initialized
    /// on first access to ensure zero overhead in JSON/Quiet modes.
    #[must_use]
    pub fn from_args(args: &Cli) -> Self {
        Self {
            mode: Self::detect_mode(args),
            width: OnceLock::new(),
            console: OnceLock::new(),
            theme: OnceLock::new(),
        }
    }

    /// Create from CLI-style flags.
    ///
    /// Only mode is set eagerly; console/theme/width are lazy-initialized
    /// on first access to ensure zero overhead in JSON/Quiet modes.
    #[must_use]
    pub fn from_flags(json: bool, quiet: bool, no_color: bool) -> Self {
        let mode = if json {
            OutputMode::Json
        } else if quiet {
            OutputMode::Quiet
        } else if no_color || std::env::var("NO_COLOR").is_ok() || !std::io::stdout().is_terminal()
        {
            OutputMode::Plain
        } else {
            OutputMode::Rich
        };

        Self {
            mode,
            width: OnceLock::new(),
            console: OnceLock::new(),
            theme: OnceLock::new(),
        }
    }

    /// Create from an explicit output format.
    #[must_use]
    pub fn from_output_format(format: OutputFormat, quiet: bool, no_color: bool) -> Self {
        let mode = match format {
            OutputFormat::Json => OutputMode::Json,
            OutputFormat::Toon => OutputMode::Toon,
            OutputFormat::Text | OutputFormat::Csv => {
                if quiet {
                    OutputMode::Quiet
                } else if no_color
                    || std::env::var("NO_COLOR").is_ok()
                    || !std::io::stdout().is_terminal()
                {
                    OutputMode::Plain
                } else {
                    OutputMode::Rich
                }
            }
        };

        Self {
            mode,
            width: OnceLock::new(),
            console: OnceLock::new(),
            theme: OnceLock::new(),
        }
    }

    fn detect_mode(args: &Cli) -> OutputMode {
        Self::detect_mode_with_env(args, OutputFormat::from_env())
    }

    fn detect_mode_with_env(args: &Cli, env_output_format: Option<OutputFormat>) -> OutputMode {
        if args.json {
            return OutputMode::Json;
        }
        if args.quiet {
            return OutputMode::Quiet;
        }
        if let Some(format) = env_output_format {
            match format {
                OutputFormat::Json => return OutputMode::Json,
                OutputFormat::Toon => return OutputMode::Toon,
                OutputFormat::Text | OutputFormat::Csv => {}
            }
        }
        if args.no_color || std::env::var("NO_COLOR").is_ok() {
            return OutputMode::Plain;
        }
        if !std::io::stdout().is_terminal() {
            return OutputMode::Plain;
        }
        OutputMode::Rich
    }

    /// Lazily create console based on mode.
    fn console(&self) -> &Console {
        self.console.get_or_init(|| match self.mode {
            OutputMode::Rich => Console::new(),
            OutputMode::Plain | OutputMode::Quiet | OutputMode::Json | OutputMode::Toon => {
                Console::builder().no_color().force_terminal(false).build()
            }
        })
    }

    // ─────────────────────────────────────────────────────────────
    // Mode Checks (no lazy initialization needed - mode is always set)
    // ─────────────────────────────────────────────────────────────

    pub fn mode(&self) -> OutputMode {
        self.mode
    }
    pub fn is_rich(&self) -> bool {
        self.mode == OutputMode::Rich
    }
    pub fn is_json(&self) -> bool {
        self.mode == OutputMode::Json
    }
    pub fn is_toon(&self) -> bool {
        self.mode == OutputMode::Toon
    }
    pub fn is_quiet(&self) -> bool {
        self.mode == OutputMode::Quiet
    }
    pub fn is_plain(&self) -> bool {
        self.mode == OutputMode::Plain
    }

    pub const fn inherited_output_mode(&self) -> InheritedOutputMode {
        match self.mode {
            OutputMode::Json => InheritedOutputMode::Json,
            OutputMode::Toon => InheritedOutputMode::Toon,
            OutputMode::Quiet => InheritedOutputMode::Quiet,
            OutputMode::Rich | OutputMode::Plain => InheritedOutputMode::None,
        }
    }

    /// Get terminal width (lazy-initialized).
    pub fn width(&self) -> usize {
        *self.width.get_or_init(|| self.console().width())
    }

    /// Get theme (lazy-initialized).
    ///
    /// In JSON/Quiet modes, this is never called, so theme is never created.
    pub fn theme(&self) -> &Theme {
        self.theme.get_or_init(Theme::default)
    }

    // ─────────────────────────────────────────────────────────────
    // Output Methods
    // ─────────────────────────────────────────────────────────────

    pub fn print(&self, content: &str) {
        match self.mode {
            OutputMode::Rich | OutputMode::Plain => {
                self.console().print(content);
            }
            OutputMode::Quiet | OutputMode::Json | OutputMode::Toon => {} // No console access - zero overhead
        }
    }

    pub fn render<R: Renderable>(&self, renderable: &R) {
        if self.is_rich() {
            self.console().print_renderable(renderable);
        }
    }

    fn report_serialization_error(&self, format: &str, err: &serde_json::Error) {
        if !self.is_quiet() {
            eprintln!("Error: failed to serialize {format} output: {err}");
        }
    }

    fn json_value<T: serde::Serialize>(
        &self,
        value: &T,
        format: &str,
    ) -> Option<serde_json::Value> {
        match serde_json::to_value(value) {
            Ok(json_value) => Some(json_value),
            Err(err) => {
                self.report_serialization_error(format, &err);
                None
            }
        }
    }

    pub fn json<T: serde::Serialize>(&self, value: &T) {
        if self.is_json() {
            // Stream to stdout to avoid allocating large JSON strings.
            let stdout = io::stdout();
            let mut out = io::BufWriter::new(stdout.lock());
            if let Err(err) = serde_json::to_writer(&mut out, value) {
                self.report_serialization_error("JSON", &err);
                return;
            }
            let _ = out.write_all(b"\n");
        }
    }

    pub fn json_pretty<T: serde::Serialize>(&self, value: &T) {
        if self.is_rich() {
            let Some(json_value) = self.json_value(value, "JSON") else {
                return;
            };
            let json = rich_rust::renderables::Json::new(json_value);
            self.console().print_renderable(&json);
        } else if self.is_json() {
            // Stream to stdout to avoid allocating large JSON strings.
            let stdout = io::stdout();
            let mut out = io::BufWriter::new(stdout.lock());
            if let Err(err) = serde_json::to_writer_pretty(&mut out, value) {
                self.report_serialization_error("JSON", &err);
                return;
            }
            let _ = out.write_all(b"\n");
        }
    }

    /// Output value as TOON format (token-optimized object notation).
    pub fn toon<T: serde::Serialize>(&self, value: &T) {
        if self.is_toon() {
            let Some(json_value) = self.json_value(value, "TOON") else {
                return;
            };
            let toon_value: JsonValue = json_value.into();
            let options = Some(EncodeOptions {
                indent: Some(2),
                delimiter: None,
                key_folding: Some(KeyFoldingMode::Safe),
                flatten_depth: None,
                replacer: None,
            });
            let toon_output = encode(toon_value, options);
            println!("{toon_output}");
        }
    }

    const fn should_emit_toon_stats(show_stats: bool, env_enabled: bool) -> bool {
        show_stats || env_enabled
    }

    fn pretty_json_len(value: &serde_json::Value) -> Option<usize> {
        let mut writer = CountingWriter::default();
        let mut serializer = serde_json::Serializer::pretty(&mut writer);
        value.serialize(&mut serializer).ok()?;
        Some(writer.len())
    }

    /// Output value as TOON format with optional stats on stderr.
    pub fn toon_with_stats<T: serde::Serialize>(&self, value: &T, show_stats: bool) {
        if self.is_toon() {
            let Some(json_value) = self.json_value(value, "TOON") else {
                return;
            };
            let emit_stats =
                Self::should_emit_toon_stats(show_stats, std::env::var("TOON_STATS").is_ok());
            let json_chars = if emit_stats {
                Self::pretty_json_len(&json_value)
            } else {
                None
            };
            let toon_value: JsonValue = json_value.into();
            let options = Some(EncodeOptions {
                indent: Some(2),
                delimiter: None,
                key_folding: Some(KeyFoldingMode::Safe),
                flatten_depth: None,
                replacer: None,
            });
            let toon_output = encode(toon_value, options);

            if let Some(json_chars) = json_chars {
                let toon_chars = toon_output.len();
                let savings = if json_chars > 0 {
                    let diff = json_chars.saturating_sub(toon_chars);
                    diff * 100 / json_chars
                } else {
                    0
                };
                eprintln!(
                    "[stats] JSON: {} chars, TOON: {} chars ({}% savings)",
                    json_chars, toon_chars, savings
                );
            }

            println!("{toon_output}");
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Semantic Output Methods
    // ─────────────────────────────────────────────────────────────

    pub fn success(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                self.console()
                    .print(&format!("[bold green]✓[/] {}", message));
            }
            OutputMode::Plain => println!("✓ {}", message),
            OutputMode::Quiet | OutputMode::Json | OutputMode::Toon => {} //
        }
    }

    pub fn error(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                let panel = Panel::from_text(message).title(Text::new("Error"));
                // .border_style(self.theme.error.clone()); // border_style missing?
                self.console().print_renderable(&panel);
            }
            OutputMode::Plain | OutputMode::Quiet => eprintln!("Error: {}", message),
            OutputMode::Json | OutputMode::Toon => {} //
        }
    }

    pub fn warning(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                self.console()
                    .print(&format!("[bold yellow]⚠[/] [yellow]{}[/]", message));
            }
            OutputMode::Plain => eprintln!("Warning: {}", message),
            OutputMode::Quiet | OutputMode::Json | OutputMode::Toon => {} //
        }
    }

    pub fn info(&self, message: &str) {
        match self.mode {
            OutputMode::Rich => {
                self.console().print(&format!("[blue]ℹ[/] {}", message));
            }
            OutputMode::Plain => println!("{}", message),
            OutputMode::Quiet | OutputMode::Json | OutputMode::Toon => {} //
        }
    }

    pub fn section(&self, title: &str) {
        if self.is_rich() {
            let rule = Rule::with_title(Text::new(title))
                // .style(self.theme.section.clone())
                ;
            self.console().print_renderable(&rule);
        } else if self.is_plain() {
            println!("\n─── {} ───\n", title);
        }
    }

    pub fn newline(&self) {
        if !self.is_quiet() && !self.is_json() && !self.is_toon() {
            println!();
        }
    }

    pub fn error_panel(&self, title: &str, description: &str, suggestions: &[&str]) {
        match self.mode {
            OutputMode::Rich => {
                let mut text = Text::from(description);
                text.append("\n\nSuggestions:\n");
                for suggestion in suggestions {
                    text.append(&format!("• {}\n", suggestion));
                }

                let panel = Panel::from_rich_text(&text, self.width()).title(Text::new(title));
                // .border_style(self.theme.error.clone());
                self.console().print_renderable(&panel);
            }
            OutputMode::Plain => {
                eprintln!("Error: {} - {}", title, description);
                for suggestion in suggestions {
                    eprintln!("  Suggestion: {}", suggestion);
                }
            }
            OutputMode::Quiet => eprintln!("Error: {}", description),
            OutputMode::Json | OutputMode::Toon => {} //
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde::Serialize;
    use serde::ser::Error as _;
    use serde_json::json;

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(S::Error::custom("boom"))
        }
    }

    #[test]
    fn detect_mode_uses_env_json_default_when_no_explicit_format_requested() {
        let cli = Cli::parse_from(["br", "count"]);
        assert_eq!(
            OutputContext::detect_mode_with_env(&cli, Some(OutputFormat::Json)),
            OutputMode::Json
        );
    }

    #[test]
    fn detect_mode_uses_env_toon_default_when_no_explicit_format_requested() {
        let cli = Cli::parse_from(["br", "count"]);
        assert_eq!(
            OutputContext::detect_mode_with_env(&cli, Some(OutputFormat::Toon)),
            OutputMode::Toon
        );
    }

    #[test]
    fn detect_mode_quiet_overrides_env_machine_format() {
        let cli = Cli::parse_from(["br", "--quiet", "count"]);
        assert_eq!(
            OutputContext::detect_mode_with_env(&cli, Some(OutputFormat::Json)),
            OutputMode::Quiet
        );
    }

    #[test]
    fn detect_mode_explicit_json_overrides_env_toon_default() {
        let cli = Cli::parse_from(["br", "--json", "count"]);
        assert_eq!(
            OutputContext::detect_mode_with_env(&cli, Some(OutputFormat::Toon)),
            OutputMode::Json
        );
    }

    #[test]
    fn should_emit_toon_stats_when_flag_is_set() {
        assert!(OutputContext::should_emit_toon_stats(true, false));
    }

    #[test]
    fn should_emit_toon_stats_when_env_is_set() {
        assert!(OutputContext::should_emit_toon_stats(false, true));
    }

    #[test]
    fn should_not_emit_toon_stats_when_flag_and_env_are_absent() {
        assert!(!OutputContext::should_emit_toon_stats(false, false));
    }

    #[test]
    fn pretty_json_len_matches_pretty_serializer_output() {
        let value = json!({
            "title": "CLI issue",
            "labels": ["cli", "perf"],
            "nested": { "priority": 2, "status": "open" }
        });

        assert_eq!(
            OutputContext::pretty_json_len(&value),
            Some(
                serde_json::to_string_pretty(&value)
                    .expect("JSON serialization failed")
                    .len()
            )
        );
    }

    #[test]
    fn json_value_returns_none_on_serialize_error() {
        let ctx = OutputContext::from_output_format(OutputFormat::Json, false, true);
        assert!(ctx.json_value(&FailingSerialize, "JSON").is_none());
    }
}
