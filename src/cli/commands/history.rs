use crate::cli::HistoryArgs;
use crate::cli::HistoryCommands;
use crate::config;
use crate::error::{BeadsError, Result};
use crate::output::OutputContext;
use crate::sync::history;
use crate::sync::{require_safe_sync_overwrite_path, validate_temp_file_path};
use rich_rust::prelude::*;
use serde_json::json;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};

/// Result type for diff status: (status_string, diff_available, optional_size_tuple).
type DiffStatusResult = (&'static str, bool, Option<(u64, u64)>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffCommandStatus {
    Identical,
    Different,
}

struct TempRestoreGuard {
    path: PathBuf,
    persist: bool,
}

impl TempRestoreGuard {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            persist: false,
        }
    }

    fn persist(&mut self) {
        self.persist = true;
    }
}

impl Drop for TempRestoreGuard {
    fn drop(&mut self) {
        if !self.persist && self.path.exists() {
            let _ = fs::remove_file(&self.path);
        }
    }
}

const MAX_RESTORE_ROLLBACK_PATH_ATTEMPTS: u64 = 1024;

fn create_restore_rollback_snapshot(
    target_path: &Path,
    beads_dir: &Path,
) -> Result<TempRestoreGuard> {
    let pid = u64::from(std::process::id());

    for offset in 1..=MAX_RESTORE_ROLLBACK_PATH_ATTEMPTS {
        let rollback_path =
            target_path.with_extension(format!("jsonl.{}.tmp", pid.saturating_add(offset)));
        validate_temp_file_path(&rollback_path, target_path, beads_dir, true)?;

        let mut writer = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&rollback_path)
        {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        };
        let rollback_guard = TempRestoreGuard::new(rollback_path);
        let mut reader = File::open(target_path)?;
        io::copy(&mut reader, &mut writer)?;
        writer.sync_all()?;
        drop(writer);
        return Ok(rollback_guard);
    }

    Err(BeadsError::Config(format!(
        "Failed to allocate rollback snapshot path for '{}'",
        target_path.display()
    )))
}

fn commit_restored_target_with_rollback<R>(
    temp_path: &Path,
    target_path: &Path,
    rollback_guard: Option<&mut TempRestoreGuard>,
    mut rename_impl: R,
) -> Result<()>
where
    R: FnMut(&Path, &Path) -> io::Result<()>,
{
    match rename_impl(temp_path, target_path) {
        Ok(()) => Ok(()),
        Err(rename_err) => {
            if let Some(rollback_guard) = rollback_guard {
                if !target_path.exists() {
                    return match fs::rename(&rollback_guard.path, target_path) {
                        Ok(()) => Err(BeadsError::Config(format!(
                            "Failed to replace '{}' with the restored backup: {rename_err}. The original target was restored.",
                            target_path.display()
                        ))),
                        Err(rollback_err) => {
                            rollback_guard.persist();
                            Err(BeadsError::Config(format!(
                                "Failed to replace '{}' with the restored backup: {rename_err}. Restoring the original target from '{}' also failed: {rollback_err}",
                                target_path.display(),
                                rollback_guard.path.display()
                            )))
                        }
                    };
                }

                rollback_guard.persist();
                return Err(BeadsError::Config(format!(
                    "Failed to replace '{}' with the restored backup: {rename_err}. The original target snapshot was preserved at '{}'.",
                    target_path.display(),
                    rollback_guard.path.display()
                )));
            }

            Err(rename_err.into())
        }
    }
}

fn emit_restore_output(
    ctx: &OutputContext,
    backup_name: &str,
    target_path: &Path,
    target_name: &str,
) {
    if ctx.is_json() {
        let output = json!({
            "action": "restore",
            "backup": backup_name,
            "target": target_path.display().to_string(),
            "restored": true,
            "next_step": "br sync --import-only --force",
        });
        ctx.json_pretty(&output);
        return;
    }

    if ctx.is_toon() {
        let output = json!({
            "action": "restore",
            "backup": backup_name,
            "target": target_path.display().to_string(),
            "restored": true,
            "next_step": "br sync --import-only --force",
        });
        ctx.toon(&output);
        return;
    }

    if ctx.is_quiet() {
        return;
    }

    if ctx.is_rich() {
        let theme = ctx.theme();
        let body = format!(
            "Restored {backup_name} to {target_name}.\nNext: br sync --import-only --force"
        );
        let panel = Panel::from_text(&body)
            .title(Text::styled("History Restore", theme.panel_title.clone()))
            .box_style(theme.box_style)
            .border_style(theme.panel_border.clone());
        ctx.render(&panel);
    } else {
        println!("Restored {backup_name} to {target_name}");
        println!("Run 'br sync --import-only --force' to import this state into the database.");
    }
}

fn ensure_regular_backup_file(backup_path: &Path, backup_name: &str) -> Result<()> {
    match fs::symlink_metadata(backup_path) {
        Ok(metadata) => {
            let file_type = metadata.file_type();
            if file_type.is_symlink() {
                return Err(BeadsError::Config(format!(
                    "History backup '{backup_name}' must not be a symlink"
                )));
            }
            if !file_type.is_file() {
                return Err(BeadsError::Config(format!(
                    "History backup '{backup_name}' must be a regular file"
                )));
            }
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Err(BeadsError::Config(format!(
            "Backup file not found: {backup_name}"
        ))),
        Err(err) => Err(err.into()),
    }
}

/// Execute the history command.
///
/// # Errors
///
/// Returns an error if history operations fail (e.g. IO error, invalid path).
pub fn execute(args: HistoryArgs, cli: &config::CliOverrides, ctx: &OutputContext) -> Result<()> {
    let beads_dir = config::discover_beads_dir_with_cli(cli)?;
    let history_dir = beads_dir.join(".br_history");
    let _ = history::validate_history_dir_path(&history_dir)?;

    match args.command {
        Some(HistoryCommands::Diff { file }) => {
            let active_jsonl_path = config::resolve_paths(&beads_dir, cli.db.as_ref())?.jsonl_path;
            diff_backup(
                &beads_dir,
                &history_dir,
                &file,
                Some(&active_jsonl_path),
                ctx,
            )
        }
        Some(HistoryCommands::Restore { file, force }) => {
            let active_jsonl_path = config::resolve_paths(&beads_dir, cli.db.as_ref())?.jsonl_path;
            restore_backup(
                &beads_dir,
                &history_dir,
                &file,
                force,
                Some(&active_jsonl_path),
                ctx,
            )
        }
        Some(HistoryCommands::Prune { keep, older_than }) => {
            prune_backups(&history_dir, keep, older_than, ctx)
        }
        Some(HistoryCommands::List) | None => list_backups(&history_dir, ctx),
    }
}

/// List available backups.
fn list_backups(history_dir: &Path, ctx: &OutputContext) -> Result<()> {
    let backups = history::list_backups(history_dir, None)?;

    if ctx.is_json() {
        let output = history_backup_list_payload(history_dir, &backups);
        ctx.json_pretty(&output);
        return Ok(());
    }

    if ctx.is_toon() {
        let output = history_backup_list_payload(history_dir, &backups);
        ctx.toon(&output);
        return Ok(());
    }

    if ctx.is_quiet() {
        return Ok(());
    }

    if backups.is_empty() {
        if ctx.is_rich() {
            let theme = ctx.theme();
            let panel = Panel::from_text("No backups found.")
                .title(Text::styled("History Backups", theme.panel_title.clone()))
                .box_style(theme.box_style)
                .border_style(theme.panel_border.clone());
            ctx.render(&panel);
        } else {
            println!("No backups found in {}", history_dir.display());
        }
        return Ok(());
    }

    if ctx.is_rich() {
        let theme = ctx.theme();
        let mut table = Table::new()
            .box_style(theme.box_style)
            .border_style(theme.panel_border.clone())
            .title(Text::styled("History Backups", theme.panel_title.clone()));

        table = table
            .with_column(Column::new("Filename").min_width(20).max_width(40))
            .with_column(Column::new("Target").min_width(24).max_width(56))
            .with_column(Column::new("Size").min_width(8).max_width(12))
            .with_column(Column::new("Timestamp").min_width(20).max_width(26));

        for entry in backups {
            let filename = entry
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            let target = entry.target_path.display().to_string();
            let size = format_size(entry.size);
            let timestamp = entry.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string();
            let row = Row::new(vec![
                Cell::new(Text::styled(filename, theme.emphasis.clone())),
                Cell::new(Text::new(target)),
                Cell::new(Text::new(size)),
                Cell::new(Text::styled(timestamp, theme.timestamp.clone())),
            ]);
            table.add_row(row);
        }

        ctx.render(&table);
    } else {
        println!("Backups in {}:", history_dir.display());
        println!(
            "{:<30} {:<36} {:<10} {:<20}",
            "FILENAME", "TARGET", "SIZE", "TIMESTAMP"
        );
        println!("{}", "-".repeat(100));

        for entry in backups {
            let filename = entry.path.file_name().unwrap_or_default().to_string_lossy();
            let target = entry.target_path.display();
            let size = format_size(entry.size);
            let timestamp = entry.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string();
            println!("{filename:<30} {target:<36} {size:<10} {timestamp:<20}");
        }
    }

    Ok(())
}

fn history_backup_list_payload(
    history_dir: &Path,
    backups: &[history::BackupEntry],
) -> serde_json::Value {
    let items: Vec<_> = backups
        .iter()
        .map(|entry| {
            let filename = entry
                .path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            json!({
                "filename": filename,
                "target": entry.target_path.display().to_string(),
                "size_bytes": entry.size,
                "size": format_size(entry.size),
                "timestamp": entry.timestamp.to_rfc3339(),
            })
        })
        .collect();

    json!({
        "directory": history_dir.display().to_string(),
        "count": backups.len(),
        "backups": items,
    })
}

/// Show diff between current state and a backup.
fn diff_backup(
    beads_dir: &Path,
    history_dir: &Path,
    filename: &str,
    active_jsonl_path: Option<&Path>,
    ctx: &OutputContext,
) -> Result<()> {
    let backup_name = validated_backup_filename(filename)?;
    let backup_path = history_dir.join(&backup_name);
    ensure_regular_backup_file(&backup_path, &backup_name)?;

    let current_path = current_jsonl_path_for_backup(beads_dir, &backup_name, active_jsonl_path)?;
    let current_name = current_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    if !current_path.exists() {
        return Err(BeadsError::Config(format!(
            "Current {current_name} not found"
        )));
    }

    if ctx.is_json() {
        let (status_label, diff_available, size_fallback) =
            diff_status_for_json(&current_path, &backup_path)?;
        let output = json!({
            "action": "diff",
            "backup": backup_name,
            "current": current_path.display().to_string(),
            "status": status_label,
            "diff_available": diff_available,
            "current_size_bytes": size_fallback.map(|sizes| sizes.0),
            "backup_size_bytes": size_fallback.map(|sizes| sizes.1),
        });
        ctx.json_pretty(&output);
        return Ok(());
    }

    if ctx.is_toon() {
        let (status_label, diff_available, size_fallback) =
            diff_status_for_json(&current_path, &backup_path)?;
        let output = json!({
            "action": "diff",
            "backup": backup_name,
            "current": current_path.display().to_string(),
            "status": status_label,
            "diff_available": diff_available,
            "current_size_bytes": size_fallback.map(|sizes| sizes.0),
            "backup_size_bytes": size_fallback.map(|sizes| sizes.1),
        });
        ctx.toon(&output);
        return Ok(());
    }

    if ctx.is_quiet() {
        return Ok(());
    }

    if ctx.is_rich() {
        let theme = ctx.theme();
        let header = format!("Current: {current_name}\nBackup: {filename}");
        let panel = Panel::from_text(&header)
            .title(Text::styled("History Diff", theme.panel_title.clone()))
            .box_style(theme.box_style)
            .border_style(theme.panel_border.clone());
        ctx.render(&panel);
    } else {
        println!("Diffing current {current_name} vs {filename}...");
    }

    // Let's shell out to `diff -u` for now as it's standard on linux/mac.
    // Avoid GNU-only flags (like --color) to keep this portable.
    let status = std::process::Command::new("diff")
        .arg("-u")
        .arg(&current_path)
        .arg(&backup_path)
        .status();

    match status {
        Ok(s) => match classify_diff_exit(s.success(), s.code())? {
            DiffCommandStatus::Identical => {
                if ctx.is_rich() {
                    ctx.success("Files are identical.");
                } else {
                    println!("Files are identical.");
                }
            }
            DiffCommandStatus::Different => {}
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let current_size = std::fs::metadata(&current_path)?.len();
            let backup_size = std::fs::metadata(&backup_path)?.len();
            let current_human = format_size(current_size);
            let backup_human = format_size(backup_size);
            if ctx.is_rich() {
                let theme = ctx.theme();
                let body = format!(
                    "Diff tool not available; comparing sizes.\nCurrent: {current_human} ({current_size} bytes)\nBackup:  {backup_human} ({backup_size} bytes)"
                );
                let panel = Panel::from_text(&body)
                    .title(Text::styled("History Diff", theme.panel_title.clone()))
                    .box_style(theme.box_style)
                    .border_style(theme.panel_border.clone());
                ctx.render(&panel);
            } else {
                println!("'diff' command not found. Comparing sizes:");
                println!("Current: {current_size} bytes");
                println!("Backup:  {backup_size} bytes");
            }
        }
        Err(err) => {
            return Err(BeadsError::Config(format!("Failed to run diff: {err}")));
        }
    }

    Ok(())
}

/// Restore a backup.
fn restore_backup(
    beads_dir: &Path,
    history_dir: &Path,
    filename: &str,
    force: bool,
    active_jsonl_path: Option<&Path>,
    ctx: &OutputContext,
) -> Result<()> {
    let backup_name = validated_backup_filename(filename)?;
    let backup_path = history_dir.join(&backup_name);
    ensure_regular_backup_file(&backup_path, &backup_name)?;

    let target_path = current_jsonl_path_for_backup(beads_dir, &backup_name, active_jsonl_path)?;
    let target_name = target_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    if target_path.exists() && !force {
        return Err(BeadsError::Config(format!(
            "Current {target_name} exists. Use --force to overwrite."
        )));
    }

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let pid = std::process::id();
    let temp_path = target_path.with_extension(format!("jsonl.{pid}.tmp"));
    validate_temp_file_path(&temp_path, &target_path, beads_dir, true)?;
    let mut reader = File::open(&backup_path)?;
    let mut writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                BeadsError::Config(format!(
                    "Temporary restore file already exists: {}",
                    temp_path.display()
                ))
            } else {
                err.into()
            }
        })?;
    let mut temp_guard = TempRestoreGuard::new(temp_path.clone());
    io::copy(&mut reader, &mut writer)?;
    writer.sync_all()?;
    drop(writer);
    let mut rollback_guard = None;
    if force && target_path.exists() {
        require_safe_sync_overwrite_path(
            &target_path,
            beads_dir,
            true,
            "overwrite history restore target",
        )?;
        rollback_guard = Some(create_restore_rollback_snapshot(&target_path, beads_dir)?);
        if let Err(err) = fs::remove_file(&target_path)
            && err.kind() != io::ErrorKind::NotFound
        {
            return Err(err.into());
        }
    }
    commit_restored_target_with_rollback(
        &temp_path,
        &target_path,
        rollback_guard.as_mut(),
        |from, to| fs::rename(from, to),
    )?;
    temp_guard.persist();
    emit_restore_output(ctx, &backup_name, &target_path, &target_name);

    Ok(())
}

/// Prune old backups.
fn prune_backups(
    history_dir: &Path,
    keep: usize,
    older_than_days: Option<u32>,
    ctx: &OutputContext,
) -> Result<()> {
    let deleted = crate::sync::history::prune_backups(history_dir, keep, older_than_days)?;

    if ctx.is_json() {
        let output = json!({
            "action": "prune",
            "deleted": deleted,
            "keep": keep,
            "older_than_days": older_than_days,
        });
        ctx.json_pretty(&output);
        return Ok(());
    }

    if ctx.is_toon() {
        let output = json!({
            "action": "prune",
            "deleted": deleted,
            "keep": keep,
            "older_than_days": older_than_days,
        });
        ctx.toon(&output);
        return Ok(());
    }

    if ctx.is_quiet() {
        return Ok(());
    }

    if ctx.is_rich() {
        let theme = ctx.theme();
        let mut body = format!("Pruned {deleted} backup(s).");
        if let Some(days) = older_than_days {
            body.push_str(&format!(
                "\nCriteria: keep {keep}, delete older than {days} days"
            ));
        } else {
            body.push_str(&format!("\nCriteria: keep {keep} newest backups"));
        }
        let panel = Panel::from_text(&body)
            .title(Text::styled("History Prune", theme.panel_title.clone()))
            .box_style(theme.box_style)
            .border_style(theme.panel_border.clone());
        ctx.render(&panel);
    } else {
        println!("Pruned {deleted} backup(s).");
    }
    Ok(())
}

fn diff_status_for_json(current_path: &Path, backup_path: &Path) -> Result<DiffStatusResult> {
    let output = std::process::Command::new("diff")
        .arg("-u")
        .arg(current_path)
        .arg(backup_path)
        .output();

    match output {
        Ok(out) => match classify_diff_exit(out.status.success(), out.status.code())? {
            DiffCommandStatus::Identical => Ok(("identical", true, None)),
            DiffCommandStatus::Different => Ok(("different", true, None)),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let current_size = std::fs::metadata(current_path)?.len();
            let backup_size = std::fs::metadata(backup_path)?.len();
            Ok(("diff_unavailable", false, Some((current_size, backup_size))))
        }
        Err(err) => Err(BeadsError::Config(format!("Failed to run diff: {err}"))),
    }
}

fn classify_diff_exit(success: bool, code: Option<i32>) -> Result<DiffCommandStatus> {
    if success {
        return Ok(DiffCommandStatus::Identical);
    }
    if code == Some(1) {
        return Ok(DiffCommandStatus::Different);
    }

    let detail = code.map_or_else(
        || "diff terminated without an exit code".to_string(),
        |value| format!("diff exited with status {value}"),
    );
    Err(BeadsError::Config(format!("Failed to run diff: {detail}")))
}

fn current_jsonl_path_for_backup(
    beads_dir: &Path,
    filename: &str,
    active_jsonl_path: Option<&Path>,
) -> Result<PathBuf> {
    let cwd = std::env::current_dir().ok();
    current_jsonl_path_for_backup_with_cwd(beads_dir, filename, active_jsonl_path, cwd.as_deref())
}

fn current_jsonl_path_for_backup_with_cwd(
    beads_dir: &Path,
    filename: &str,
    active_jsonl_path: Option<&Path>,
    cwd: Option<&Path>,
) -> Result<PathBuf> {
    let backup_name = validated_backup_filename(filename)?;
    let target_path = history::resolve_backup_target_path(
        beads_dir,
        &beads_dir.join(".br_history").join(backup_name),
    )?;
    let canonical_beads =
        dunce::canonicalize(beads_dir).unwrap_or_else(|_| beads_dir.to_path_buf());
    let is_external_target =
        !target_path.starts_with(beads_dir) && !target_path.starts_with(&canonical_beads);

    if is_external_target {
        let active_jsonl_path = active_jsonl_path.ok_or_else(|| {
            BeadsError::Config(format!(
                "External backup target '{}' requires the current active JSONL path",
                target_path.display()
            ))
        })?;
        let normalized_target = normalize_jsonl_match_path(&target_path, cwd);
        let normalized_active = normalize_jsonl_match_path(active_jsonl_path, cwd);
        let canonical_target =
            dunce::canonicalize(&normalized_target).unwrap_or_else(|_| normalized_target.clone());
        let canonical_active =
            dunce::canonicalize(&normalized_active).unwrap_or_else(|_| normalized_active.clone());
        if canonical_target != canonical_active {
            return Err(BeadsError::Config(format!(
                "Backup target '{}' does not match the active JSONL path '{}'",
                target_path.display(),
                active_jsonl_path.display()
            )));
        }
    }

    Ok(target_path)
}

fn normalize_jsonl_match_path(path: &Path, cwd: Option<&Path>) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else if let Some(cwd) = cwd {
        cwd.join(path)
    } else {
        path.to_path_buf()
    }
}

fn validated_backup_filename(filename: &str) -> Result<String> {
    let mut components = Path::new(filename).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(name)), None) => {
            name.to_str().map(str::to_string).ok_or_else(|| {
                BeadsError::Config(format!("Invalid backup filename format: {filename}"))
            })
        }
        _ => Err(BeadsError::Config(format!(
            "Invalid backup filename format: {filename}"
        ))),
    }
}

#[allow(clippy::cast_precision_loss)]
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;

    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputContext;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_current_jsonl_path_for_backup_rejects_missing_target_metadata() {
        let temp = TempDir::new().unwrap();
        let history_dir = temp.path().join(".br_history");
        fs::create_dir_all(&history_dir).unwrap();
        fs::write(
            history_dir.join("issues.20260220_120000.jsonl"),
            "backup-state\n",
        )
        .unwrap();

        let err = current_jsonl_path_for_backup(temp.path(), "issues.20260220_120000.jsonl", None)
            .unwrap_err();
        match err {
            BeadsError::Config(msg) => {
                assert!(
                    msg.contains("missing target metadata"),
                    "unexpected message: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_current_jsonl_path_for_backup_rejects_invalid_name() {
        let temp = TempDir::new().unwrap();
        let err = current_jsonl_path_for_backup(temp.path(), "issues.not-a-timestamp.jsonl", None)
            .unwrap_err();

        match err {
            BeadsError::Config(msg) => assert!(msg.contains("Invalid backup filename format")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_current_jsonl_path_for_backup_rejects_path_traversal() {
        let temp = TempDir::new().unwrap();
        let err =
            current_jsonl_path_for_backup(temp.path(), "../issues.20260220_120000.jsonl", None)
                .unwrap_err();

        match err {
            BeadsError::Config(msg) => assert!(msg.contains("Invalid backup filename format")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_classify_diff_exit_rejects_real_diff_failures() {
        let err = classify_diff_exit(false, Some(2)).unwrap_err();

        match err {
            BeadsError::Config(msg) => assert!(msg.contains("diff exited with status 2")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_restore_backup_uses_metadata_target_path() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        fs::create_dir_all(&beads_dir).unwrap();

        let target_path = beads_dir.join("custom.jsonl");
        fs::write(&target_path, "new-state\n").unwrap();
        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &target_path).unwrap();

        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");
        fs::write(&target_path, "old-state\n").unwrap();

        let ctx = OutputContext::from_flags(false, true, true);
        restore_backup(&beads_dir, &history_dir, &backup_name, true, None, &ctx).unwrap();

        assert_eq!(
            fs::read_to_string(beads_dir.join("custom.jsonl")).unwrap(),
            "new-state\n"
        );
        assert!(!beads_dir.join("issues.jsonl").exists());
    }

    #[test]
    fn test_current_jsonl_path_for_backup_reads_target_metadata() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        let external_dir = temp.path().join("external");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::create_dir_all(&external_dir).unwrap();

        let external_target = external_dir.join("issues.jsonl");
        fs::write(&external_target, "external-state\n").unwrap();

        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &external_target).unwrap();

        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");

        let resolved =
            current_jsonl_path_for_backup(&beads_dir, &backup_name, Some(&external_target))
                .unwrap();
        assert_eq!(resolved, external_target);
    }

    #[test]
    fn test_current_jsonl_path_for_backup_accepts_relative_external_active_path_when_missing() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        let external_dir = temp.path().join("external");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::create_dir_all(&external_dir).unwrap();

        let external_target = external_dir.join("issues.jsonl");
        fs::write(&external_target, "external-state\n").unwrap();

        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &external_target).unwrap();

        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");

        fs::remove_file(&external_target).unwrap();

        let resolved = current_jsonl_path_for_backup_with_cwd(
            &beads_dir,
            &backup_name,
            Some(Path::new("external/issues.jsonl")),
            Some(temp.path()),
        )
        .unwrap();
        assert_eq!(resolved, external_target);
    }

    #[test]
    fn test_restore_backup_recreates_missing_target_parent_directories() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        let nested_target = beads_dir.join("snapshots").join("issues.jsonl");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::create_dir_all(nested_target.parent().unwrap()).unwrap();
        fs::write(&nested_target, "nested-state\n").unwrap();

        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &nested_target).unwrap();

        fs::remove_dir_all(nested_target.parent().unwrap()).unwrap();

        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");

        let ctx = OutputContext::from_flags(false, true, true);
        restore_backup(&beads_dir, &history_dir, &backup_name, true, None, &ctx).unwrap();

        assert_eq!(
            fs::read_to_string(&nested_target).unwrap(),
            "nested-state\n"
        );
    }

    #[test]
    fn test_restore_backup_cleans_temp_file_when_rename_fails() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        fs::create_dir_all(&beads_dir).unwrap();

        let target_path = beads_dir.join("issues.jsonl");
        fs::write(&target_path, "restored-state\n").unwrap();
        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &target_path).unwrap();

        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");
        let target_dir = beads_dir.join("issues.jsonl");
        fs::remove_file(&target_dir).unwrap();
        fs::create_dir_all(&target_dir).unwrap();
        fs::write(target_dir.join("occupied.txt"), "keep").unwrap();

        let ctx = OutputContext::from_flags(false, true, true);
        let err =
            restore_backup(&beads_dir, &history_dir, &backup_name, true, None, &ctx).unwrap_err();
        assert!(
            matches!(err, BeadsError::Io(_) | BeadsError::Config(_)),
            "unexpected error: {err}"
        );
        let pid = std::process::id();
        assert!(
            !beads_dir.join(format!("issues.jsonl.{pid}.tmp")).exists(),
            "failed restore should clean up the temporary restore file"
        );
    }

    #[test]
    fn test_commit_restored_target_restores_original_file_when_replace_fails() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let target_path = beads_dir.join("issues.jsonl");
        let temp_path = beads_dir.join("issues.jsonl.100.tmp");
        fs::write(&target_path, "original-state\n").unwrap();
        fs::write(&temp_path, "restored-state\n").unwrap();

        let mut rollback_guard =
            create_restore_rollback_snapshot(&target_path, &beads_dir).unwrap();
        fs::remove_file(&target_path).unwrap();

        let err = commit_restored_target_with_rollback(
            &temp_path,
            &target_path,
            Some(&mut rollback_guard),
            |_from, _to| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "forced failure",
                ))
            },
        )
        .unwrap_err();

        match err {
            BeadsError::Config(message) => {
                assert!(
                    message.contains("original target was restored"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(
            fs::read_to_string(&target_path).unwrap(),
            "original-state\n"
        );
        assert_eq!(fs::read_to_string(&temp_path).unwrap(), "restored-state\n");
        assert!(!rollback_guard.path.exists());
    }

    #[test]
    fn test_current_jsonl_path_for_backup_rejects_tampered_absolute_metadata() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        fs::create_dir_all(&history_dir).unwrap();

        let backup_name = "issues.20260220_120000.jsonl";
        let backup_path = history_dir.join(backup_name);
        fs::write(&backup_path, "backup\n").unwrap();
        fs::write(
            backup_path.with_extension("jsonl.meta.json"),
            serde_json::json!({
                "target": {
                    "kind": "absolute",
                    "path": temp.path().join("escape.txt").display().to_string(),
                }
            })
            .to_string(),
        )
        .unwrap();

        let active_jsonl_path = beads_dir.join("issues.jsonl");
        let err = current_jsonl_path_for_backup(&beads_dir, backup_name, Some(&active_jsonl_path))
            .unwrap_err();
        match err {
            BeadsError::Config(msg) => {
                assert!(
                    msg.contains(".jsonl")
                        || msg.contains("traversal")
                        || msg.contains("regular file")
                        || msg.contains("Path"),
                    "unexpected message: {msg}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_diff_backup_reports_missing_current_stem_file() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        fs::create_dir_all(&beads_dir).unwrap();

        let target_path = beads_dir.join("custom.jsonl");
        fs::write(&target_path, "backup\n").unwrap();
        let config = history::HistoryConfig {
            enabled: true,
            max_count: 10,
            max_age_days: 30,
        };
        history::backup_before_export(&beads_dir, &config, &target_path).unwrap();
        let backup_name = history::list_backups(&history_dir, None)
            .unwrap()
            .into_iter()
            .next()
            .and_then(|entry| {
                entry
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .expect("backup filename");
        fs::remove_file(&target_path).unwrap();

        let ctx = OutputContext::from_flags(false, true, true);
        let err = diff_backup(&beads_dir, &history_dir, &backup_name, None, &ctx).unwrap_err();

        match err {
            BeadsError::Config(msg) => assert!(msg.contains("Current custom.jsonl not found")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_restore_backup_rejects_symlinked_backup_file() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let history_dir = beads_dir.join(".br_history");
        let outside_dir = temp.path().join("outside");
        fs::create_dir_all(&history_dir).unwrap();
        fs::create_dir_all(&outside_dir).unwrap();

        let backup_name = "issues.20260220_120000.jsonl";
        let outside_backup = outside_dir.join("backup.jsonl");
        fs::write(&outside_backup, "backup\n").unwrap();
        symlink(&outside_backup, history_dir.join(backup_name)).unwrap();

        let ctx = OutputContext::from_flags(false, true, true);
        let err =
            restore_backup(&beads_dir, &history_dir, backup_name, true, None, &ctx).unwrap_err();
        match err {
            BeadsError::Config(msg) => assert!(msg.contains("must not be a symlink")),
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
