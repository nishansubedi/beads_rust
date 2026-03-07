//! Where command implementation.

use crate::config;
use crate::config::routing::follow_redirects;
use crate::error::Result;
use crate::output::OutputContext;
use crate::util::parse_id;
use rich_rust::prelude::*;
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize)]
struct WhereOutput {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    redirected_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jsonl_path: Option<String>,
}

/// Execute the where command.
///
/// # Errors
///
/// Returns an error if redirect resolution fails.
pub fn execute(cli: &config::CliOverrides, ctx: &OutputContext) -> Result<()> {
    let Some(output) = resolve_where_output(cli)? else {
        return handle_missing_beads(ctx);
    };

    if ctx.is_json() {
        ctx.json_pretty(&output);
    } else if ctx.is_rich() {
        render_where_rich(&output, ctx);
    } else {
        print_human(&output);
    }

    Ok(())
}

fn resolve_where_output(cli: &config::CliOverrides) -> Result<Option<WhereOutput>> {
    let Some(beads_dir) = config::discover_optional_beads_dir_with_cli(cli)? else {
        return Ok(None);
    };

    let final_dir = follow_redirects(&beads_dir, 10)?;
    let redirected_from = if final_dir == beads_dir {
        None
    } else {
        Some(canonicalize_lossy(&beads_dir).display().to_string())
    };

    let paths = config::ConfigPaths::resolve(&final_dir, cli.db.as_ref())?;
    let database_path = canonicalize_lossy(&paths.db_path).display().to_string();
    let jsonl_path = canonicalize_lossy(&paths.jsonl_path).display().to_string();
    let prefix = detect_prefix(&final_dir, &paths.jsonl_path, cli);

    Ok(Some(WhereOutput {
        path: canonicalize_lossy(&final_dir).display().to_string(),
        redirected_from,
        prefix,
        database_path: Some(database_path),
        jsonl_path: Some(jsonl_path),
    }))
}

fn detect_prefix(
    beads_dir: &Path,
    jsonl_path: &Path,
    cli: &config::CliOverrides,
) -> Option<String> {
    if let Ok(storage_ctx) = config::open_storage_with_cli(beads_dir, cli) {
        if let Ok(Some(prefix)) = storage_ctx.storage.get_config("issue_prefix")
            && !prefix.trim().is_empty()
        {
            return Some(prefix);
        }

        if let Ok(ids) = storage_ctx.storage.get_all_ids()
            && let Some(prefix) = ids
                .first()
                .and_then(|id| parse_id(id).ok().map(|parsed| parsed.prefix))
        {
            return Some(prefix);
        }
    }

    prefix_from_jsonl(jsonl_path)
}

fn prefix_from_jsonl(path: &Path) -> Option<String> {
    if !path.is_file() {
        return None;
    }

    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines().map_while(std::result::Result::ok) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(id) = value.get("id").and_then(|value| value.as_str()) else {
            continue;
        };
        if let Ok(parsed) = parse_id(id) {
            return Some(parsed.prefix);
        }
    }

    None
}

fn print_human(output: &WhereOutput) {
    println!("{}", output.path);
    if let Some(origin) = &output.redirected_from {
        println!("  (via redirect from {origin})");
    }
    if let Some(prefix) = &output.prefix {
        println!("  prefix: {prefix}");
    }
    if let Some(db_path) = &output.database_path {
        println!("  database: {db_path}");
    }
}

fn handle_missing_beads(ctx: &OutputContext) -> Result<()> {
    if ctx.is_json() {
        let payload = serde_json::json!({ "error": "no beads directory found" });
        ctx.json_pretty(&payload);
    } else if ctx.is_rich() {
        let console = Console::default();
        let theme = ctx.theme();
        let mut text = Text::new("");
        text.append_styled("\u{2717} ", theme.error.clone());
        text.append_styled("No beads directory found.\n", theme.error.clone());
        text.append_styled("  Run ", theme.muted.clone());
        text.append_styled("br init", theme.accent.clone());
        text.append_styled(" to create one.", theme.muted.clone());
        console.print_renderable(&text);
    } else {
        eprintln!("No beads directory found.");
        eprintln!("Run `br init` to create one.");
    }
    std::process::exit(1);
}

/// Render location info as a rich panel.
fn render_where_rich(output: &WhereOutput, ctx: &OutputContext) {
    let console = Console::default();
    let theme = ctx.theme();
    let width = ctx.width();

    let mut content = Text::new("");

    // Main path
    content.append_styled("Directory   ", theme.dimmed.clone());
    content.append_styled(&output.path, theme.accent.clone());
    content.append("\n");

    // Redirect info
    if let Some(origin) = &output.redirected_from {
        content.append_styled("            ", theme.dimmed.clone());
        content.append_styled("(via redirect from ", theme.muted.clone());
        content.append_styled(origin, theme.accent.clone());
        content.append_styled(")\n", theme.muted.clone());
    }

    // Prefix
    if let Some(prefix) = &output.prefix {
        content.append_styled("Prefix      ", theme.dimmed.clone());
        content.append_styled(prefix, theme.issue_id.clone());
        content.append("\n");
    }

    // Database path
    if let Some(db_path) = &output.database_path {
        content.append_styled("Database    ", theme.dimmed.clone());
        content.append_styled(db_path, theme.accent.clone());
        content.append("\n");
    }

    // JSONL path
    if let Some(jsonl_path) = &output.jsonl_path {
        content.append_styled("JSONL       ", theme.dimmed.clone());
        content.append_styled(jsonl_path, theme.accent.clone());
        content.append("\n");
    }

    let title = output
        .prefix
        .as_ref()
        .map_or_else(|| "Beads Location".to_string(), |p| format!("{p} Location"));

    let panel = Panel::from_rich_text(&content, width)
        .title(Text::styled(&title, theme.panel_title.clone()))
        .box_style(theme.box_style);

    console.print_renderable(&panel);
}

fn canonicalize_lossy(path: &Path) -> PathBuf {
    dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliOverrides;
    use crate::error::BeadsError;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn resolve_where_output_uses_explicit_db_override() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join("external").join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::write(&jsonl_path, r#"{"id":"proj-abc12","title":"Example"}"#).expect("write jsonl");

        let cli = CliOverrides {
            db: Some(db_path.clone()),
            ..CliOverrides::default()
        };

        let output = resolve_where_output(&cli)
            .expect("where output")
            .expect("workspace output");

        assert_eq!(
            output.path,
            canonicalize_lossy(&beads_dir).display().to_string()
        );
        assert_eq!(
            output.database_path,
            Some(canonicalize_lossy(&db_path).display().to_string())
        );
        assert_eq!(
            output.jsonl_path,
            Some(canonicalize_lossy(&jsonl_path).display().to_string())
        );
        assert_eq!(output.prefix.as_deref(), Some("proj"));
    }

    #[test]
    fn resolve_where_output_surfaces_invalid_db_override() {
        let cli = CliOverrides {
            db: Some(PathBuf::from("/tmp/not-a-beads-db")),
            ..CliOverrides::default()
        };

        let err = resolve_where_output(&cli).expect_err("invalid db override should error");
        assert!(matches!(err, BeadsError::Validation { .. }));
    }
}
