//! Doctor command implementation.

#![allow(clippy::option_if_let_else)]

use crate::cli::DoctorArgs;
use crate::config;
use crate::error::{BeadsError, Result};
use crate::output::OutputContext;
use crate::storage::SqliteStorage;
use crate::sync::{
    PathValidation, compute_staleness, scan_conflict_markers, validate_no_git_path,
    validate_sync_path, validate_sync_path_with_external,
};
use fsqlite::Connection;
use fsqlite_error::FrankenError;
use fsqlite_types::SqliteValue;
use rich_rust::prelude::*;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Check result status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum CheckStatus {
    Ok,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize)]
struct CheckResult {
    name: String,
    status: CheckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<CheckResult>,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct DoctorRepairResult {
    imported: usize,
    skipped: usize,
    fk_violations_cleaned: usize,
}

#[derive(Debug, Clone)]
struct DoctorRun {
    report: DoctorReport,
    jsonl_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Serialize)]
struct LocalRepairResult {
    blocked_cache_rebuilt: bool,
    wal_checkpoint_completed: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    quarantined_artifacts: Vec<String>,
}

const BLOCKED_CACHE_STALE_FINDING: &str = "blocked_issues_cache is marked stale and needs rebuild";

#[derive(Debug, Default)]
struct SidecarInspection {
    findings: Vec<String>,
    quarantine_candidates: Vec<PathBuf>,
    wal_requires_reconciliation: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilesystemPathKind {
    Missing,
    File,
    Directory,
    Symlink,
    Other,
}

impl LocalRepairResult {
    fn applied(&self) -> bool {
        self.blocked_cache_rebuilt
            || self.wal_checkpoint_completed
            || !self.quarantined_artifacts.is_empty()
    }
}

impl FilesystemPathKind {
    fn exists(self) -> bool {
        !matches!(self, Self::Missing)
    }

    fn is_regular_file(self) -> bool {
        matches!(self, Self::File)
    }

    fn description(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::File => "regular file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
            Self::Other => "special filesystem entry",
        }
    }
}

fn push_check(
    checks: &mut Vec<CheckResult>,
    name: &str,
    status: CheckStatus,
    message: Option<String>,
    details: Option<serde_json::Value>,
) {
    checks.push(CheckResult {
        name: name.to_string(),
        status,
        message,
        details,
    });
}

fn has_error(checks: &[CheckResult]) -> bool {
    checks
        .iter()
        .any(|check| matches!(check.status, CheckStatus::Error))
}

fn report_has_blocked_cache_stale_finding(report: &DoctorReport) -> bool {
    report.checks.iter().any(|check| {
        if check.name != "db.recoverable_anomalies" {
            return false;
        }

        if check
            .message
            .as_deref()
            .is_some_and(|message| message.contains(BLOCKED_CACHE_STALE_FINDING))
        {
            return true;
        }

        check
            .details
            .as_ref()
            .and_then(|details| details.get("findings"))
            .and_then(serde_json::Value::as_array)
            .is_some_and(|findings| {
                findings.iter().any(|finding| {
                    finding
                        .as_str()
                        .is_some_and(|message| message.contains(BLOCKED_CACHE_STALE_FINDING))
                })
            })
    })
}

fn report_has_sidecar_anomaly(report: &DoctorReport) -> bool {
    report
        .checks
        .iter()
        .any(|check| check.name == "db.sidecars" && matches!(check.status, CheckStatus::Error))
}

fn local_repair_message(local_repair: &LocalRepairResult) -> String {
    let mut actions = Vec::new();
    if local_repair.blocked_cache_rebuilt {
        actions.push("rebuilt the stale blocked cache".to_string());
    }
    if local_repair.wal_checkpoint_completed {
        actions.push("checkpointed database WAL state".to_string());
    }
    if !local_repair.quarantined_artifacts.is_empty() {
        actions.push(format!(
            "quarantined {} anomalous database artifact(s)",
            local_repair.quarantined_artifacts.len()
        ));
    }

    if actions.is_empty() {
        "No remaining errors detected after recoverable-state repair.".to_string()
    } else {
        format!("Repair complete: {}.", actions.join("; "))
    }
}

fn classify_path_kind(path: &Path) -> Result<FilesystemPathKind> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Ok(FilesystemPathKind::Symlink),
        Ok(metadata) if metadata.is_file() => Ok(FilesystemPathKind::File),
        Ok(metadata) if metadata.is_dir() => Ok(FilesystemPathKind::Directory),
        Ok(_) => Ok(FilesystemPathKind::Other),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(FilesystemPathKind::Missing),
        Err(err) => Err(err.into()),
    }
}

fn database_sidecar_paths(db_path: &Path) -> [(PathBuf, &'static str); 3] {
    let db_string = db_path.to_string_lossy();
    [
        (PathBuf::from(format!("{db_string}-wal")), "WAL"),
        (PathBuf::from(format!("{db_string}-shm")), "SHM"),
        (
            PathBuf::from(format!("{db_string}-journal")),
            "rollback journal",
        ),
    ]
}

fn inspect_database_sidecars(db_path: &Path) -> Result<SidecarInspection> {
    let db_kind = classify_path_kind(db_path)?;
    let mut inspection = SidecarInspection::default();
    let mut wal_kind = FilesystemPathKind::Missing;
    let mut shm_kind = FilesystemPathKind::Missing;

    for (path, label) in database_sidecar_paths(db_path) {
        let kind = classify_path_kind(&path)?;
        match label {
            "WAL" => wal_kind = kind,
            "SHM" => shm_kind = kind,
            _ => {}
        }

        if kind.exists() && !db_kind.is_regular_file() {
            inspection.quarantine_candidates.push(path.clone());
        }

        if kind.exists() && !kind.is_regular_file() {
            inspection.findings.push(format!(
                "{label} sidecar at {} is a {} instead of a regular file",
                path.display(),
                kind.description()
            ));
            inspection.quarantine_candidates.push(path);
        }
    }

    if wal_kind.is_regular_file() && !shm_kind.exists() {
        let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        inspection.findings.push(format!(
            "WAL sidecar exists without a matching SHM sidecar at {}",
            wal_path.display()
        ));
        inspection.wal_requires_reconciliation = true;
        inspection.quarantine_candidates.push(wal_path);
    }

    if shm_kind.is_regular_file() && !wal_kind.exists() {
        let shm_path = PathBuf::from(format!("{}-shm", db_path.to_string_lossy()));
        inspection.findings.push(format!(
            "SHM sidecar exists without a matching WAL sidecar at {}",
            shm_path.display()
        ));
        inspection.quarantine_candidates.push(shm_path);
    }

    if !db_kind.is_regular_file() {
        let has_dangling_sidecars = database_sidecar_paths(db_path)
            .into_iter()
            .any(|(path, _)| {
                classify_path_kind(&path)
                    .ok()
                    .is_some_and(FilesystemPathKind::exists)
            });
        if has_dangling_sidecars {
            inspection.findings.push(format!(
                "Database sidecars exist even though the primary database at {} is a {}",
                db_path.display(),
                db_kind.description()
            ));
        }
    }

    inspection.quarantine_candidates.sort();
    inspection.quarantine_candidates.dedup();
    Ok(inspection)
}

fn check_database_sidecars(db_path: &Path, checks: &mut Vec<CheckResult>) -> Result<()> {
    let inspection = inspect_database_sidecars(db_path)?;
    if inspection.findings.is_empty() {
        push_check(checks, "db.sidecars", CheckStatus::Ok, None, None);
        return Ok(());
    }

    push_check(
        checks,
        "db.sidecars",
        CheckStatus::Error,
        Some(inspection.findings[0].clone()),
        Some(serde_json::json!({
            "findings": inspection.findings,
            "quarantine_candidates": inspection
                .quarantine_candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>(),
        })),
    );
    Ok(())
}

fn check_recovery_artifacts(
    beads_dir: &Path,
    db_path: &Path,
    checks: &mut Vec<CheckResult>,
) -> Result<()> {
    let recovery_dir = config::recovery_dir_for_db_path(db_path, beads_dir);
    let db_prefix = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("beads.db");
    let db_parent = db_path.parent().unwrap_or(beads_dir);
    let mut artifacts = Vec::new();

    if recovery_dir.is_dir() {
        for entry in fs::read_dir(&recovery_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(db_prefix) {
                artifacts.push(entry.path().display().to_string());
            }
        }
    }

    for entry in fs::read_dir(db_parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{db_prefix}.bad_")) {
            artifacts.push(entry.path().display().to_string());
        }
    }

    artifacts.sort();
    artifacts.dedup();

    if artifacts.is_empty() {
        push_check(checks, "db.recovery_artifacts", CheckStatus::Ok, None, None);
    } else {
        push_check(
            checks,
            "db.recovery_artifacts",
            CheckStatus::Warn,
            Some(format!(
                "Preserved recovery artifacts remain for this database family ({} item(s))",
                artifacts.len()
            )),
            Some(serde_json::json!({ "artifacts": artifacts })),
        );
    }

    Ok(())
}

fn push_inspection_error(
    checks: &mut Vec<CheckResult>,
    name: &str,
    context: &str,
    err: &BeadsError,
) {
    push_check(
        checks,
        name,
        CheckStatus::Error,
        Some(format!("{context}: {err}")),
        None,
    );
}

fn build_issue_write_probe_check(
    issue_id: &str,
    update_result: std::result::Result<usize, FrankenError>,
    rollback_result: std::result::Result<usize, FrankenError>,
) -> CheckResult {
    let mut details = serde_json::json!({ "issue_id": issue_id });

    match (update_result, rollback_result) {
        (Ok(_), Ok(_)) => CheckResult {
            name: "db.write_probe".to_string(),
            status: CheckStatus::Ok,
            message: Some(format!(
                "Rollback-only issue write succeeded for {issue_id}"
            )),
            details: None,
        },
        (Ok(_), Err(rollback_err)) => {
            details["rollback_error"] = serde_json::json!(rollback_err.to_string());
            CheckResult {
                name: "db.write_probe".to_string(),
                status: CheckStatus::Error,
                message: Some(format!(
                    "Rollback-only issue write succeeded but rollback failed: {rollback_err}"
                )),
                details: Some(details),
            }
        }
        (Err(update_err), Ok(_)) => CheckResult {
            name: "db.write_probe".to_string(),
            status: CheckStatus::Error,
            message: Some(format!("Rollback-only issue write failed: {update_err}")),
            details: Some(details),
        },
        (Err(update_err), Err(rollback_err)) => {
            details["rollback_error"] = serde_json::json!(rollback_err.to_string());
            CheckResult {
                name: "db.write_probe".to_string(),
                status: CheckStatus::Error,
                message: Some(format!(
                    "Rollback-only issue write failed and rollback also failed: {update_err}"
                )),
                details: Some(details),
            }
        }
    }
}

fn repair_database_from_jsonl(
    beads_dir: &Path,
    db_path: &Path,
    jsonl_path: &Path,
    cli: &config::CliOverrides,
    show_progress: bool,
) -> Result<DoctorRepairResult> {
    let bootstrap_layer = config::ConfigLayer::merge_layers(&[
        config::load_startup_config(beads_dir)?,
        cli.as_layer(),
    ]);

    let (storage, import_result) = config::repair_database_from_jsonl(
        beads_dir,
        db_path,
        jsonl_path,
        cli.lock_timeout,
        &bootstrap_layer,
        show_progress,
    )?;

    let fk_violations = storage.execute_raw_query("PRAGMA foreign_key_check")?.len();

    if fk_violations > 0 {
        tracing::warn!(
            violations = fk_violations,
            "FK violations found after repair import; cleaning orphans"
        );
        for table in &[
            "dependencies",
            "labels",
            "comments",
            "events",
            "dirty_issues",
            "export_hashes",
            "blocked_issues_cache",
            "child_counters",
        ] {
            let col = if *table == "child_counters" {
                "parent_id"
            } else {
                "issue_id"
            };
            let cleanup = format!("DELETE FROM {table} WHERE {col} NOT IN (SELECT id FROM issues)");
            storage.execute_raw(&cleanup)?;
        }

        let remaining_fk_violations = storage.execute_raw_query("PRAGMA foreign_key_check")?.len();
        if remaining_fk_violations > 0 {
            return Err(BeadsError::Config(format!(
                "Repair import finished with {remaining_fk_violations} foreign key violation(s) still present"
            )));
        }
    }

    Ok(DoctorRepairResult {
        imported: import_result.imported_count,
        skipped: import_result.skipped_count,
        fk_violations_cleaned: fk_violations,
    })
}

fn repair_recoverable_db_state(
    beads_dir: &Path,
    db_path: &Path,
    report: &DoctorReport,
) -> LocalRepairResult {
    let mut repair = LocalRepairResult::default();

    if report_has_sidecar_anomaly(report) {
        repair_database_sidecars(beads_dir, db_path, &mut repair);
    }

    if !db_path.is_file() {
        tracing::debug!(
            path = %db_path.display(),
            "Skipping blocked-cache repair because the database file is missing"
        );
        return repair;
    }

    match SqliteStorage::open(db_path) {
        Ok(storage) => match storage.ensure_blocked_cache_fresh() {
            Ok(blocked_cache_rebuilt) => {
                repair.blocked_cache_rebuilt = blocked_cache_rebuilt;
                repair
            }
            Err(err) => {
                tracing::warn!(
                    path = %db_path.display(),
                    error = %err,
                    "Skipping blocked-cache repair; falling back to JSONL rebuild"
                );
                repair
            }
        },
        Err(err) => {
            tracing::warn!(
                path = %db_path.display(),
                error = %err,
                "Skipping blocked-cache repair because the database could not be opened"
            );
            repair
        }
    }
}

fn repair_database_sidecars(beads_dir: &Path, db_path: &Path, repair: &mut LocalRepairResult) {
    match inspect_database_sidecars(db_path) {
        Ok(initial_inspection) => {
            checkpoint_anomalous_wal(db_path, &initial_inspection, repair);
            quarantine_anomalous_sidecars(beads_dir, db_path, repair);
        }
        Err(err) => tracing::warn!(
            path = %db_path.display(),
            error = %err,
            "Skipping sidecar repair because filesystem inspection failed"
        ),
    }
}

fn checkpoint_anomalous_wal(
    db_path: &Path,
    inspection: &SidecarInspection,
    repair: &mut LocalRepairResult,
) {
    if !inspection.wal_requires_reconciliation || !db_path.is_file() {
        return;
    }

    match SqliteStorage::open(db_path) {
        Ok(storage) => match storage.execute_raw("PRAGMA wal_checkpoint(TRUNCATE)") {
            Ok(()) => repair.wal_checkpoint_completed = true,
            Err(err) => tracing::warn!(
                path = %db_path.display(),
                error = %err,
                "Failed to checkpoint anomalous WAL sidecar before quarantine"
            ),
        },
        Err(err) => tracing::warn!(
            path = %db_path.display(),
            error = %err,
            "Failed to open database while reconciling anomalous sidecars"
        ),
    }
}

fn quarantine_anomalous_sidecars(beads_dir: &Path, db_path: &Path, repair: &mut LocalRepairResult) {
    match inspect_database_sidecars(db_path) {
        Ok(post_checkpoint_inspection) => {
            let mut quarantine_paths: BTreeSet<_> = post_checkpoint_inspection
                .quarantine_candidates
                .into_iter()
                .collect();
            let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));

            if post_checkpoint_inspection.wal_requires_reconciliation
                && !repair.wal_checkpoint_completed
                && quarantine_paths.remove(&wal_path)
            {
                tracing::warn!(
                    path = %wal_path.display(),
                    "Skipping WAL quarantine because checkpoint reconciliation did not succeed"
                );
            }

            if !quarantine_paths.is_empty() {
                match config::quarantine_database_artifacts(
                    db_path,
                    beads_dir,
                    quarantine_paths,
                    "doctor-quarantine",
                ) {
                    Ok(quarantined) => {
                        repair.quarantined_artifacts = quarantined
                            .into_iter()
                            .map(|path| path.display().to_string())
                            .collect();
                    }
                    Err(err) => tracing::warn!(
                        path = %db_path.display(),
                        error = %err,
                        "Failed to quarantine anomalous database sidecar artifacts"
                    ),
                }
            }
        }
        Err(err) => tracing::warn!(
            path = %db_path.display(),
            error = %err,
            "Failed to re-inspect database sidecars after local repair"
        ),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn print_report(report: &DoctorReport, ctx: &OutputContext) -> Result<()> {
    if ctx.is_json() {
        ctx.json(report);
        return Ok(());
    }
    if ctx.is_quiet() {
        return Ok(());
    }
    if ctx.is_rich() {
        render_doctor_rich(report, ctx);
        return Ok(());
    }

    print_report_plain(report);
    Ok(())
}

fn print_report_plain(report: &DoctorReport) {
    println!("br doctor");
    for check in &report.checks {
        let label = match check.status {
            CheckStatus::Ok => "OK",
            CheckStatus::Warn => "WARN",
            CheckStatus::Error => "ERROR",
        };
        if let Some(message) = &check.message {
            println!("{label} {}: {}", check.name, message);
        } else {
            println!("{label} {}", check.name);
        }
    }
}

fn render_doctor_rich(report: &DoctorReport, ctx: &OutputContext) {
    let theme = ctx.theme();
    let mut content = Text::new("");

    let mut ok_count = 0usize;
    let mut warn_count = 0usize;
    let mut error_count = 0usize;
    for check in &report.checks {
        match check.status {
            CheckStatus::Ok => ok_count += 1,
            CheckStatus::Warn => warn_count += 1,
            CheckStatus::Error => error_count += 1,
        }
    }

    content.append_styled("Diagnostics Report\n", theme.emphasis.clone());
    content.append("\n");

    content.append_styled("Status: ", theme.dimmed.clone());
    if report.ok {
        content.append_styled("OK", theme.success.clone());
    } else {
        content.append_styled("Issues found", theme.error.clone());
    }
    content.append("\n");

    content.append_styled("Checks: ", theme.dimmed.clone());
    content.append_styled(
        &format!("{ok_count} ok, {warn_count} warn, {error_count} error"),
        theme.accent.clone(),
    );
    content.append("\n\n");

    for check in &report.checks {
        let (label, style) = match check.status {
            CheckStatus::Ok => ("[OK]", theme.success.clone()),
            CheckStatus::Warn => ("[WARN]", theme.warning.clone()),
            CheckStatus::Error => ("[ERROR]", theme.error.clone()),
        };

        content.append_styled(label, style);
        content.append(" ");
        content.append_styled(&check.name, theme.issue_title.clone());
        if let Some(message) = &check.message {
            content.append_styled(": ", theme.dimmed.clone());
            content.append(message);
        }
        content.append("\n");

        if !matches!(check.status, CheckStatus::Ok)
            && let Some(details) = &check.details
            && let Ok(details_text) = serde_json::to_string_pretty(details)
        {
            for line in details_text.lines() {
                content.append_styled("    ", theme.dimmed.clone());
                content.append_styled(line, theme.dimmed.clone());
                content.append("\n");
            }
        }
    }

    let panel = Panel::from_rich_text(&content, ctx.width())
        .title(Text::styled("Doctor", theme.panel_title.clone()))
        .box_style(theme.box_style)
        .border_style(theme.panel_border.clone());

    ctx.render(&panel);
}

fn collect_table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let rows = conn.query(&format!("PRAGMA table_info({table})"))?;
    let mut columns = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(name) = row.get(1).and_then(SqliteValue::as_text) {
            columns.push(name.to_string());
        }
    }
    Ok(columns)
}

#[allow(clippy::too_many_lines)]
fn required_schema_checks(conn: &Connection, checks: &mut Vec<CheckResult>) -> Result<()> {
    let rows = conn
        .query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
    let mut tables = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(name) = row.get(0).and_then(SqliteValue::as_text) {
            tables.push(name.to_string());
        }
    }

    let required_tables = [
        "issues",
        "dependencies",
        "labels",
        "comments",
        "events",
        "config",
        "metadata",
        "dirty_issues",
        "export_hashes",
        "blocked_issues_cache",
        "child_counters",
    ];

    // Fallback: if sqlite_master returned nothing (frankensqlite may not
    // support it), probe each required table directly.
    if tables.is_empty() {
        for &table in &required_tables {
            let probe = format!("SELECT 1 FROM {table} LIMIT 1");
            if conn.query(&probe).is_ok() {
                tables.push(table.to_string());
            }
        }
    }

    let missing_tables: Vec<&str> = required_tables
        .iter()
        .copied()
        .filter(|table| !tables.iter().any(|t| t == table))
        .collect();

    if missing_tables.is_empty() {
        push_check(
            checks,
            "schema.tables",
            CheckStatus::Ok,
            None,
            Some(serde_json::json!({ "tables": tables })),
        );
    } else {
        push_check(
            checks,
            "schema.tables",
            CheckStatus::Error,
            Some(format!("Missing tables: {}", missing_tables.join(", "))),
            Some(serde_json::json!({ "missing": missing_tables })),
        );
    }

    let required_columns: &[(&str, &[&str])] = &[
        (
            "issues",
            &[
                "id",
                "title",
                "status",
                "priority",
                "issue_type",
                "created_at",
                "updated_at",
            ],
        ),
        (
            "dependencies",
            &["issue_id", "depends_on_id", "type", "created_at"],
        ),
        (
            "comments",
            &["id", "issue_id", "author", "text", "created_at"],
        ),
        (
            "events",
            &["id", "issue_id", "event_type", "actor", "created_at"],
        ),
    ];

    let mut missing_columns = Vec::new();
    for (table, cols) in required_columns {
        let present = collect_table_columns(conn, table)?;
        let missing: Vec<&str> = cols
            .iter()
            .copied()
            .filter(|col| !present.iter().any(|p| p == col))
            .collect();
        if !missing.is_empty() {
            missing_columns.push(serde_json::json!({
                "table": table,
                "missing": missing,
            }));
        }
    }

    if missing_columns.is_empty() {
        push_check(checks, "schema.columns", CheckStatus::Ok, None, None);
    } else {
        push_check(
            checks,
            "schema.columns",
            CheckStatus::Error,
            Some("Missing required columns".to_string()),
            Some(serde_json::json!({ "tables": missing_columns })),
        );
    }

    Ok(())
}

fn check_integrity(conn: &Connection, checks: &mut Vec<CheckResult>) {
    let rows = match conn.query("PRAGMA integrity_check") {
        Ok(rows) => rows,
        Err(err) => {
            push_check(
                checks,
                "sqlite.integrity_check",
                CheckStatus::Error,
                Some(err.to_string()),
                None,
            );
            return;
        }
    };

    let row_values: Vec<Vec<SqliteValue>> = rows.iter().map(|row| row.values().to_vec()).collect();
    let messages = integrity_check_messages(&row_values);
    if messages.len() == 1 && messages[0].trim().eq_ignore_ascii_case("ok") {
        push_check(
            checks,
            "sqlite.integrity_check",
            CheckStatus::Ok,
            None,
            None,
        );
    } else {
        push_check(
            checks,
            "sqlite.integrity_check",
            CheckStatus::Error,
            Some(messages.join("; ")),
            (messages.len() > 1).then(|| serde_json::json!({ "messages": messages })),
        );
    }
}

fn check_recoverable_anomalies(conn: &Connection, checks: &mut Vec<CheckResult>) -> Result<()> {
    let duplicate_schema_rows = conn.query(
        "SELECT type, name, COUNT(*) AS row_count
         FROM sqlite_master
         WHERE name IN ('blocked_issues_cache', 'idx_blocked_cache_blocked_at')
         GROUP BY type, name
         HAVING COUNT(*) > 1
         ORDER BY row_count DESC, name ASC
         LIMIT 1",
    )?;

    let duplicate_config = conn.query(
        "SELECT key, COUNT(*) AS row_count
         FROM config
         GROUP BY key
         HAVING COUNT(*) > 1
         ORDER BY row_count DESC, key ASC
         LIMIT 1",
    )?;

    let duplicate_metadata = conn.query(
        "SELECT key, COUNT(*) AS row_count
         FROM metadata
         GROUP BY key
         HAVING COUNT(*) > 1
         ORDER BY row_count DESC, key ASC
         LIMIT 1",
    )?;

    let blocked_cache_stale = conn.query(
        "SELECT value
         FROM metadata
         WHERE key = 'blocked_cache_state'
         LIMIT 1",
    )?;

    let mut findings = Vec::new();

    if let Some(row) = duplicate_schema_rows.first() {
        let object_type = row
            .get(0)
            .and_then(SqliteValue::as_text)
            .unwrap_or("object");
        let name = row
            .get(1)
            .and_then(SqliteValue::as_text)
            .unwrap_or("unknown");
        let row_count = row.get(2).and_then(SqliteValue::as_integer).unwrap_or(2);
        findings.push(format!(
            "sqlite_master contains duplicate {object_type} entries for '{name}' ({row_count} rows)"
        ));
    }

    if let Some(row) = duplicate_config.first() {
        let key = row
            .get(0)
            .and_then(SqliteValue::as_text)
            .unwrap_or("unknown");
        let row_count = row.get(1).and_then(SqliteValue::as_integer).unwrap_or(2);
        findings.push(format!(
            "config contains duplicate rows for key '{key}' ({row_count} rows)"
        ));
    }

    if let Some(row) = duplicate_metadata.first() {
        let key = row
            .get(0)
            .and_then(SqliteValue::as_text)
            .unwrap_or("unknown");
        let row_count = row.get(1).and_then(SqliteValue::as_integer).unwrap_or(2);
        findings.push(format!(
            "metadata contains duplicate rows for key '{key}' ({row_count} rows)"
        ));
    }

    if blocked_cache_stale
        .first()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text))
        == Some("stale")
    {
        findings.push(BLOCKED_CACHE_STALE_FINDING.to_string());
    }

    if findings.is_empty() {
        push_check(
            checks,
            "db.recoverable_anomalies",
            CheckStatus::Ok,
            None,
            None,
        );
    } else {
        push_check(
            checks,
            "db.recoverable_anomalies",
            CheckStatus::Error,
            Some(findings[0].clone()),
            Some(serde_json::json!({ "findings": findings })),
        );
    }

    Ok(())
}

/// Check for NULL values in NOT NULL columns that should have DEFAULTs.
///
/// Detects rows inserted before the `DEFAULT ''` was added to the schema
/// (e.g., events.actor or comments.author without DEFAULT).
fn check_null_defaults(conn: &Connection, checks: &mut Vec<CheckResult>) {
    let queries: &[(&str, &str, &str)] = &[
        (
            "events",
            "actor",
            "UPDATE events SET actor = '' WHERE actor IS NULL",
        ),
        (
            "events",
            "created_at",
            "UPDATE events SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL",
        ),
        (
            "comments",
            "created_at",
            "UPDATE comments SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL",
        ),
    ];

    let mut null_findings = Vec::new();

    for (table, column, fix_sql) in queries {
        let count_sql = format!(
            "SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
        );
        match conn.query_row(&count_sql) {
            Ok(row) => {
                let count = row.get(0).and_then(SqliteValue::as_integer).unwrap_or(0);
                if count > 0 {
                    null_findings.push(serde_json::json!({
                        "table": table,
                        "column": column,
                        "null_count": count,
                        "fix_sql": fix_sql,
                    }));
                }
            }
            Err(_) => {
                // Table might not exist yet; skip silently
            }
        }
    }

    if null_findings.is_empty() {
        push_check(checks, "db.null_defaults", CheckStatus::Ok, None, None);
    } else {
        let first = &null_findings[0];
        let table = first["table"].as_str().unwrap_or("?");
        let column = first["column"].as_str().unwrap_or("?");
        let count = first["null_count"].as_i64().unwrap_or(0);
        push_check(
            checks,
            "db.null_defaults",
            CheckStatus::Warn,
            Some(format!(
                "{table}.{column} has {count} NULL value(s); fix with: {}",
                first["fix_sql"].as_str().unwrap_or("see details")
            )),
            Some(serde_json::json!({ "findings": null_findings })),
        );
    }
}

fn check_issue_write_probe(conn: &Connection, checks: &mut Vec<CheckResult>) {
    let issue_id = match conn.query_row("SELECT id FROM issues ORDER BY id LIMIT 1") {
        Ok(row) => row
            .get(0)
            .and_then(SqliteValue::as_text)
            .map(ToString::to_string),
        Err(FrankenError::QueryReturnedNoRows) => None,
        Err(err) => {
            push_check(
                checks,
                "db.write_probe",
                CheckStatus::Error,
                Some(format!("Failed to select probe issue: {err}")),
                None,
            );
            return;
        }
    };

    let Some(issue_id) = issue_id else {
        push_check(
            checks,
            "db.write_probe",
            CheckStatus::Ok,
            Some("No issues available for rollback-only write probe".to_string()),
            None,
        );
        return;
    };

    let begin_result = conn.execute("BEGIN IMMEDIATE");
    if let Err(err) = begin_result {
        let status = if err.is_transient() {
            CheckStatus::Warn
        } else {
            CheckStatus::Error
        };
        push_check(
            checks,
            "db.write_probe",
            status,
            Some(format!("Failed to begin rollback-only write probe: {err}")),
            Some(serde_json::json!({ "issue_id": issue_id })),
        );
        return;
    }

    let update_result = conn.execute_with_params(
        "UPDATE issues SET priority = priority, status = status WHERE id = ?",
        &[SqliteValue::from(issue_id.as_str())],
    );
    let rollback_result = conn.execute("ROLLBACK");

    checks.push(build_issue_write_probe_check(
        &issue_id,
        update_result,
        rollback_result,
    ));
}

fn check_live_issue_write_probe(db_path: &Path, checks: &mut Vec<CheckResult>) {
    match Connection::open(db_path.to_string_lossy().into_owned()) {
        Ok(conn) => {
            let _ = conn.execute("PRAGMA busy_timeout=0");
            check_issue_write_probe(&conn, checks);
        }
        Err(err) => push_check(
            checks,
            "db.write_probe",
            CheckStatus::Error,
            Some(format!(
                "Failed to open live DB for rollback-only write probe: {err}"
            )),
            Some(serde_json::json!({ "path": db_path.display().to_string() })),
        ),
    }
}

fn sqlite_cli_integrity_messages(db_path: &Path) -> Result<Vec<String>> {
    let output = Command::new("sqlite3")
        .arg(db_path)
        .arg("PRAGMA integrity_check;")
        .output()
        .map_err(|err| BeadsError::Config(format!("failed to run sqlite3: {err}")))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut messages: Vec<String> = stdout
        .lines()
        .chain(stderr.lines())
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect();

    if messages.is_empty() && !output.status.success() {
        messages.push(format!(
            "sqlite3 exited with status {}",
            output.status.code().unwrap_or(-1)
        ));
    }

    if output.status.success() {
        Ok(messages)
    } else {
        Err(BeadsError::Config(messages.join("; ")))
    }
}

fn check_sqlite_cli_integrity(db_path: &Path, checks: &mut Vec<CheckResult>) {
    match sqlite_cli_integrity_messages(db_path) {
        Ok(messages) if messages.len() == 1 && messages[0].eq_ignore_ascii_case("ok") => {
            push_check(
                checks,
                "sqlite3.integrity_check",
                CheckStatus::Ok,
                None,
                None,
            );
        }
        Ok(messages) => {
            push_check(
                checks,
                "sqlite3.integrity_check",
                CheckStatus::Error,
                Some(messages.join("; ")),
                (messages.len() > 1).then(|| serde_json::json!({ "messages": messages })),
            );
        }
        Err(BeadsError::Config(message))
            if message.contains("No such file or directory")
                || message.contains("failed to run sqlite3") =>
        {
            push_check(
                checks,
                "sqlite3.integrity_check",
                CheckStatus::Warn,
                Some("sqlite3 not available; skipping orthogonal integrity validation".to_string()),
                None,
            );
        }
        Err(err) => {
            push_check(
                checks,
                "sqlite3.integrity_check",
                CheckStatus::Error,
                Some(err.to_string()),
                None,
            );
        }
    }
}

fn integrity_check_messages(rows: &[Vec<SqliteValue>]) -> Vec<String> {
    let mut messages = Vec::new();
    for row in rows {
        for value in row {
            if let Some(text) = value.as_text() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    messages.push(trimmed.to_string());
                }
            }
        }
    }

    if messages.is_empty() {
        messages.push("integrity_check returned no diagnostic rows".to_string());
    }

    messages
}

fn check_merge_artifacts(beads_dir: &Path, checks: &mut Vec<CheckResult>) -> Result<()> {
    let mut artifacts = Vec::new();
    for entry in beads_dir.read_dir()? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.contains(".base.jsonl")
            || name.contains(".left.jsonl")
            || name.contains(".right.jsonl")
        {
            artifacts.push(name.to_string());
        }
    }

    if artifacts.is_empty() {
        push_check(checks, "jsonl.merge_artifacts", CheckStatus::Ok, None, None);
    } else {
        push_check(
            checks,
            "jsonl.merge_artifacts",
            CheckStatus::Warn,
            Some("Merge artifacts detected in .beads/".to_string()),
            Some(serde_json::json!({ "files": artifacts })),
        );
    }
    Ok(())
}

fn discover_jsonl(beads_dir: &Path) -> Option<PathBuf> {
    let issues = beads_dir.join("issues.jsonl");
    if issues.exists() {
        return Some(issues);
    }
    let legacy = beads_dir.join("beads.jsonl");
    if legacy.exists() {
        return Some(legacy);
    }
    None
}

fn should_fallback_to_workspace_jsonl(beads_dir: &Path, paths: &config::ConfigPaths) -> bool {
    let has_env_override = std::env::var("BEADS_JSONL")
        .ok()
        .is_some_and(|value| !value.trim().is_empty());

    !has_env_override
        && paths.metadata.jsonl_export == "issues.jsonl"
        && paths.jsonl_path == beads_dir.join("issues.jsonl")
}

fn select_doctor_jsonl_path(beads_dir: &Path, paths: &config::ConfigPaths) -> Option<PathBuf> {
    if paths.jsonl_path.exists() {
        Some(paths.jsonl_path.clone())
    } else if should_fallback_to_workspace_jsonl(beads_dir, paths) {
        discover_jsonl(beads_dir)
    } else {
        Some(paths.jsonl_path.clone())
    }
}

fn check_jsonl(path: &Path, checks: &mut Vec<CheckResult>) -> Result<usize> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut total = 0usize;
    let mut invalid = Vec::new();
    let mut invalid_count = 0usize;

    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        total += 1;
        if serde_json::from_str::<serde_json::Value>(trimmed).is_err() {
            invalid_count += 1;
            if invalid.len() < 10 {
                invalid.push(idx + 1);
            }
        }
    }

    if invalid.is_empty() {
        push_check(
            checks,
            "jsonl.parse",
            CheckStatus::Ok,
            Some(format!("Parsed {total} records")),
            Some(serde_json::json!({
                "path": path.display().to_string(),
                "records": total
            })),
        );
    } else {
        push_check(
            checks,
            "jsonl.parse",
            CheckStatus::Error,
            Some(format!(
                "Malformed JSONL lines: {invalid_count} (first: {invalid:?})"
            )),
            Some(serde_json::json!({
                "path": path.display().to_string(),
                "records": total,
                "invalid_lines": invalid,
                "invalid_count": invalid_count
            })),
        );
    }

    Ok(total)
}

fn check_db_count(
    conn: &Connection,
    jsonl_count: Option<usize>,
    checks: &mut Vec<CheckResult>,
) -> Result<()> {
    let db_count: i64 = conn.query_row(
        "SELECT count(*) FROM issues WHERE (ephemeral = 0 OR ephemeral IS NULL) AND id NOT LIKE '%-wisp-%'",
    )?
        .get(0)
        .and_then(SqliteValue::as_integer)
        .unwrap_or(0);

    if let Some(jsonl_count) = jsonl_count {
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let db_count_usize = db_count as usize;
        if db_count_usize == jsonl_count {
            push_check(
                checks,
                "counts.db_vs_jsonl",
                CheckStatus::Ok,
                Some(format!("Both have {db_count} records")),
                None,
            );
        } else {
            push_check(
                checks,
                "counts.db_vs_jsonl",
                CheckStatus::Warn,
                Some("DB and JSONL counts differ".to_string()),
                Some(serde_json::json!({
                    "db": db_count,
                    "jsonl": jsonl_count
                })),
            );
        }
    } else {
        push_check(
            checks,
            "counts.db_vs_jsonl",
            CheckStatus::Warn,
            Some("JSONL not found; cannot compare counts".to_string()),
            Some(serde_json::json!({ "db": db_count })),
        );
    }

    Ok(())
}

// ============================================================================
// SYNC SAFETY CHECKS (beads_rust-0v1.2.6)
// ============================================================================

/// Check if the JSONL path is within the sync allowlist.
///
/// This validates that the JSONL path:
/// 1. Does not target git internals (.git/)
/// 2. Is within the .beads directory, or passes the configured external-path policy
/// 3. Has an allowed extension
#[allow(clippy::too_many_lines)]
fn check_sync_jsonl_path(jsonl_path: &Path, beads_dir: &Path, checks: &mut Vec<CheckResult>) {
    let check_name = "sync_jsonl_path";

    // 1. Check if path is valid UTF-8
    if let Some(_name) = jsonl_path.file_name().and_then(|n| n.to_str()) {
        // 2. Check for git path access (critical safety invariant)
        let git_check = validate_no_git_path(jsonl_path);
        if !git_check.is_allowed() {
            let reason = git_check.rejection_reason().unwrap_or_default();
            push_check(
                checks,
                check_name,
                CheckStatus::Error,
                Some(format!("JSONL path targets git internals: {reason}")),
                Some(serde_json::json!({
                    "path": jsonl_path.display().to_string(),
                    "reason": reason,
                    "remediation": "Move JSONL file inside .beads/ directory"
                })),
            );
            return;
        }

        let is_external = config::resolved_jsonl_path_is_external(beads_dir, jsonl_path);
        if is_external {
            match validate_sync_path_with_external(jsonl_path, beads_dir, true) {
                Ok(()) => {
                    push_check(
                        checks,
                        check_name,
                        CheckStatus::Ok,
                        Some("Configured external JSONL path is valid for sync I/O".to_string()),
                        Some(serde_json::json!({
                            "path": jsonl_path.display().to_string(),
                            "beads_dir": beads_dir.display().to_string(),
                            "external": true
                        })),
                    );
                }
                Err(err) => {
                    push_check(
                        checks,
                        check_name,
                        CheckStatus::Error,
                        Some(format!("Configured external JSONL path is invalid: {err}")),
                        Some(serde_json::json!({
                            "path": jsonl_path.display().to_string(),
                            "beads_dir": beads_dir.display().to_string(),
                            "external": true
                        })),
                    );
                }
            }
            return;
        }

        // 3. Check if path is within beads_dir allowlist
        let path_validation = validate_sync_path(jsonl_path, beads_dir);
        match path_validation {
            PathValidation::Allowed => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Ok,
                    Some("JSONL path is within sync allowlist".to_string()),
                    Some(serde_json::json!({
                        "path": jsonl_path.display().to_string(),
                        "beads_dir": beads_dir.display().to_string()
                    })),
                );
            }
            PathValidation::OutsideBeadsDir {
                path,
                beads_dir: bd,
            } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Warn,
                    Some("JSONL path is outside .beads/ directory".to_string()),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "beads_dir": bd.display().to_string(),
                        "remediation": "Use --allow-external-jsonl flag or move JSONL inside .beads/"
                    })),
                );
            }
            PathValidation::DisallowedExtension { path, extension } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some(format!("JSONL path has disallowed extension: {extension}")),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "extension": extension,
                        "remediation": "Use a .jsonl extension for JSONL files"
                    })),
                );
            }
            PathValidation::TraversalAttempt { path } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some("JSONL path contains traversal sequences".to_string()),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "remediation": "Remove '..' sequences from path"
                    })),
                );
            }
            PathValidation::SymlinkEscape { path, target } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some("JSONL path is a symlink pointing outside .beads/".to_string()),
                    Some(serde_json::json!({
                        "symlink": path.display().to_string(),
                        "target": target.display().to_string(),
                        "remediation": "Remove symlink and use a regular file inside .beads/"
                    })),
                );
            }
            PathValidation::CanonicalizationFailed { path, error } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Warn,
                    Some(format!("Could not verify JSONL path: {error}")),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "error": error
                    })),
                );
            }
            PathValidation::NonRegularFile { path } => {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some("JSONL path is not a regular file".to_string()),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "remediation": "Replace the path with a regular .jsonl file"
                    })),
                );
            }
            PathValidation::GitPathAttempt { path } => {
                // Already handled above, but include for completeness
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some("JSONL path targets git internals".to_string()),
                    Some(serde_json::json!({
                        "path": path.display().to_string(),
                        "remediation": "Move JSONL file inside .beads/ directory"
                    })),
                );
            }
        }
    } else {
        push_check(
            checks,
            check_name,
            CheckStatus::Error,
            Some("Invalid JSONL path (not valid UTF-8)".to_string()),
            Some(serde_json::json!({
                "path": jsonl_path.display().to_string(),
                "remediation": "Ensure the path is valid UTF-8"
            })),
        );
    }
}

/// Check for git merge conflict markers in the JSONL file.
///
/// Conflict markers indicate an unresolved merge and must be resolved
/// before any sync operations can proceed safely.
#[allow(clippy::unnecessary_wraps)]
fn check_sync_conflict_markers(jsonl_path: &Path, checks: &mut Vec<CheckResult>) {
    let check_name = "sync_conflict_markers";

    if !jsonl_path.exists() {
        return;
    }

    match scan_conflict_markers(jsonl_path) {
        Ok(markers) => {
            if markers.is_empty() {
                push_check(
                    checks,
                    check_name,
                    CheckStatus::Ok,
                    Some("No merge conflict markers found".to_string()),
                    None,
                );
            } else {
                // Format first few markers for display
                let preview: Vec<serde_json::Value> = markers
                    .iter()
                    .take(5)
                    .map(|m| {
                        serde_json::json!({
                            "line": m.line,
                            "type": format!("{:?}", m.marker_type),
                            "branch": m.branch.as_deref().unwrap_or("")
                        })
                    })
                    .collect();

                push_check(
                    checks,
                    check_name,
                    CheckStatus::Error,
                    Some(format!(
                        "Found {} merge conflict marker(s) in JSONL",
                        markers.len()
                    )),
                    Some(serde_json::json!({
                        "path": jsonl_path.display().to_string(),
                        "count": markers.len(),
                        "markers_preview": preview,
                        "remediation": "Resolve git merge conflicts in the JSONL file before running sync"
                    })),
                );
            }
        }
        Err(e) => {
            push_check(
                checks,
                check_name,
                CheckStatus::Warn,
                Some(format!("Could not scan for conflict markers: {e}")),
                Some(serde_json::json!({
                    "path": jsonl_path.display().to_string(),
                    "error": e.to_string()
                })),
            );
        }
    }
}

/// Check sync metadata consistency.
///
/// Validates that sync-related metadata is consistent and not stale.
#[allow(clippy::too_many_lines)]
fn check_sync_metadata(
    conn: &Connection,
    db_path: &Path,
    jsonl_path: Option<&Path>,
    checks: &mut Vec<CheckResult>,
) {
    // Get metadata for diagnostic details
    let last_import: Option<String> = conn
        .query_row("SELECT value FROM metadata WHERE key = 'last_import_time'")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text).map(String::from));

    let last_export: Option<String> = conn
        .query_row("SELECT value FROM metadata WHERE key = 'last_export_time'")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text).map(String::from));

    let jsonl_hash: Option<String> = conn
        .query_row("SELECT value FROM metadata WHERE key = 'jsonl_content_hash'")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text).map(String::from));

    // Check dirty issues count
    let dirty_count: i64 = conn
        .query_row("SELECT count(*) FROM dirty_issues")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
        .unwrap_or(0);

    let mut details = serde_json::json!({
        "dirty_issues": dirty_count
    });

    if let Some(ts) = &last_import {
        details["last_import"] = serde_json::json!(ts);
    }
    if let Some(ts) = &last_export {
        details["last_export"] = serde_json::json!(ts);
    }
    if let Some(hash) = &jsonl_hash {
        details["jsonl_hash"] = serde_json::json!(&hash[..16.min(hash.len())]);
    }

    // Determine staleness using the canonical compute_staleness() from sync module.
    // This avoids duplicating logic that accounts for last_export_time, mtime witness
    // fast-path, and content hash verification (issue #173).
    let (jsonl_newer, db_newer) = if let Some(p) = jsonl_path {
        match SqliteStorage::open(db_path).and_then(|storage| compute_staleness(&storage, p)) {
            Ok(staleness) => (staleness.jsonl_newer, staleness.db_newer),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "compute_staleness failed in doctor; falling back to dirty-count only"
                );
                (false, dirty_count > 0)
            }
        }
    } else {
        (false, dirty_count > 0)
    };

    // Check 1: Metadata consistency
    if last_export.is_none() && dirty_count > 0 {
        push_check(
            checks,
            "sync.metadata",
            CheckStatus::Warn,
            Some(
                "JSONL exists but no export recorded; consider running sync --flush-only"
                    .to_string(),
            ),
            Some(details),
        );
    } else {
        match (jsonl_newer, db_newer) {
            (false, false) => {
                push_check(
                    checks,
                    "sync.metadata",
                    CheckStatus::Ok,
                    Some("Database and JSONL are in sync".to_string()),
                    Some(details),
                );
            }
            (true, false) => {
                push_check(
                    checks,
                    "sync.metadata",
                    CheckStatus::Ok, // Acceptable state
                    Some("External changes pending import".to_string()),
                    Some(details),
                );
            }
            (false, true) => {
                push_check(
                    checks,
                    "sync.metadata",
                    CheckStatus::Ok, // Acceptable state
                    Some("Local changes pending export".to_string()),
                    Some(details),
                );
            }
            (true, true) => {
                push_check(
                    checks,
                    "sync.metadata",
                    CheckStatus::Warn,
                    Some("Database and JSONL have diverged (merge required)".to_string()),
                    Some(details),
                );
            }
        }
    }
}

fn collect_doctor_report(beads_dir: &Path, paths: &config::ConfigPaths) -> Result<DoctorRun> {
    let mut checks = Vec::new();
    check_merge_artifacts(beads_dir, &mut checks)?;

    let (jsonl_path, jsonl_count) = inspect_doctor_jsonl(beads_dir, paths, &mut checks);
    inspect_doctor_database(
        beads_dir,
        &paths.db_path,
        jsonl_path.as_deref(),
        jsonl_count,
        &mut checks,
    );

    Ok(DoctorRun {
        report: DoctorReport {
            ok: !has_error(&checks),
            checks,
        },
        jsonl_path,
    })
}

fn inspect_doctor_jsonl(
    beads_dir: &Path,
    paths: &config::ConfigPaths,
    checks: &mut Vec<CheckResult>,
) -> (Option<PathBuf>, Option<usize>) {
    let jsonl_path = select_doctor_jsonl_path(beads_dir, paths);
    let jsonl_count = if let Some(path) = jsonl_path.as_ref() {
        check_sync_jsonl_path(path, beads_dir, checks);
        check_sync_conflict_markers(path, checks);

        match check_jsonl(path, checks) {
            Ok(count) => Some(count),
            Err(err) => {
                push_check(
                    checks,
                    "jsonl.parse",
                    CheckStatus::Error,
                    Some(format!("Failed to read JSONL: {err}")),
                    Some(serde_json::json!({ "path": path.display().to_string() })),
                );
                None
            }
        }
    } else {
        push_check(
            checks,
            "jsonl.parse",
            CheckStatus::Warn,
            Some("No JSONL file found (.beads/issues.jsonl or .beads/beads.jsonl)".to_string()),
            None,
        );
        None
    };

    (jsonl_path, jsonl_count)
}

fn inspect_doctor_database(
    beads_dir: &Path,
    db_path: &Path,
    jsonl_path: Option<&Path>,
    jsonl_count: Option<usize>,
    checks: &mut Vec<CheckResult>,
) {
    if let Err(err) = check_recovery_artifacts(beads_dir, db_path, checks) {
        push_inspection_error(
            checks,
            "db.recovery_artifacts",
            "Failed to inspect preserved recovery artifacts",
            &err,
        );
    }
    if let Err(err) = check_database_sidecars(db_path, checks) {
        push_inspection_error(
            checks,
            "db.sidecars",
            "Failed to inspect database sidecars",
            &err,
        );
    }

    if db_path.exists() {
        inspect_existing_doctor_database(db_path, jsonl_path, jsonl_count, checks);
    } else {
        push_check(
            checks,
            "db.exists",
            CheckStatus::Error,
            Some(format!("Missing database file at {}", db_path.display())),
            Some(serde_json::json!({ "path": db_path.display().to_string() })),
        );
    }
}

fn inspect_existing_doctor_database(
    db_path: &Path,
    jsonl_path: Option<&Path>,
    jsonl_count: Option<usize>,
    checks: &mut Vec<CheckResult>,
) {
    match config::with_database_family_snapshot(db_path, |snapshot_db_path| {
        let conn = Connection::open(snapshot_db_path.to_string_lossy().into_owned())?;
        let _ = conn.execute("PRAGMA busy_timeout=30000");
        if let Err(err) = required_schema_checks(&conn, checks) {
            push_inspection_error(
                checks,
                "schema.inspect",
                "Failed to inspect database schema",
                &err,
            );
        }
        if let Err(err) = check_recoverable_anomalies(&conn, checks) {
            push_inspection_error(
                checks,
                "db.recoverable_anomalies",
                "Failed to inspect recoverable anomalies",
                &err,
            );
        }
        check_null_defaults(&conn, checks);
        check_integrity(&conn, checks);
        if let Err(err) = check_db_count(&conn, jsonl_count, checks) {
            push_inspection_error(
                checks,
                "counts.db_vs_jsonl",
                "Failed to compare database and JSONL counts",
                &err,
            );
        }
        check_sync_metadata(&conn, snapshot_db_path, jsonl_path, checks);
        Ok(())
    }) {
        Ok(()) => {
            check_live_issue_write_probe(db_path, checks);
            check_sqlite_cli_integrity(db_path, checks);
        }
        Err(err) => {
            push_check(
                checks,
                "db.open",
                CheckStatus::Error,
                Some(format!("Failed to open DB snapshot for inspection: {err}")),
                Some(serde_json::json!({ "path": db_path.display().to_string() })),
            );
            check_sqlite_cli_integrity(db_path, checks);
        }
    }
}

/// Execute the doctor command.
///
/// # Errors
///
/// Returns an error if report serialization fails or if IO operations fail.
#[allow(clippy::too_many_lines)]
pub fn execute(args: &DoctorArgs, cli: &config::CliOverrides, ctx: &OutputContext) -> Result<()> {
    let Some(beads_dir) = config::discover_optional_beads_dir_with_cli(cli)? else {
        let mut checks = Vec::new();
        push_check(
            &mut checks,
            "beads_dir",
            CheckStatus::Error,
            Some("Missing .beads directory (run `br init`)".to_string()),
            None,
        );
        let report = DoctorReport {
            ok: !has_error(&checks),
            checks,
        };
        print_report(&report, ctx)?;
        std::process::exit(1);
    };

    let paths = match config::resolve_paths(&beads_dir, cli.db.as_ref()) {
        Ok(paths) => paths,
        Err(err) => {
            let mut checks = Vec::new();
            push_check(
                &mut checks,
                "metadata",
                CheckStatus::Error,
                Some(format!("Failed to read metadata.json: {err}")),
                None,
            );
            let report = DoctorReport {
                ok: !has_error(&checks),
                checks,
            };
            print_report(&report, ctx)?;
            std::process::exit(1);
        }
    };

    let initial = collect_doctor_report(&beads_dir, &paths)?;

    if !args.repair {
        print_report(&initial.report, ctx)?;
        if !initial.report.ok {
            std::process::exit(1);
        }
        return Ok(());
    }

    if initial.report.ok {
        if ctx.is_json() {
            ctx.json(&serde_json::json!({
                "report": initial.report,
                "repaired": false,
                "message": "No errors detected; nothing to repair."
            }));
        } else {
            print_report(&initial.report, ctx)?;
            ctx.info("No errors detected; nothing to repair.");
        }
        return Ok(());
    }

    let local_repair = if report_has_blocked_cache_stale_finding(&initial.report)
        || report_has_sidecar_anomaly(&initial.report)
    {
        repair_recoverable_db_state(&beads_dir, &paths.db_path, &initial.report)
    } else {
        LocalRepairResult::default()
    };
    let after_local_repair = if local_repair.applied() {
        collect_doctor_report(&beads_dir, &paths)?
    } else {
        initial.clone()
    };

    if after_local_repair.report.ok {
        let repair_message = local_repair_message(&local_repair);
        if ctx.is_json() {
            ctx.json(&serde_json::json!({
                "report": initial.report,
                "repaired": local_repair.applied(),
                "local_repair": local_repair,
                "message": repair_message,
                "post_repair": after_local_repair.report,
                "verified": true,
            }));
        } else {
            print_report(&initial.report, ctx)?;
            ctx.info(&repair_message);
            ctx.info("Post-repair verification:");
            print_report(&after_local_repair.report, ctx)?;
        }
        return Ok(());
    }

    let Some(jsonl_path) = initial.jsonl_path.as_ref() else {
        return Err(BeadsError::Config(
            "Cannot repair: no JSONL file found to rebuild from".to_string(),
        ));
    };

    if !ctx.is_json() {
        print_report(&initial.report, ctx)?;
        ctx.info("Repairing: rebuilding DB from JSONL...");
    }

    let repair_result =
        repair_database_from_jsonl(&beads_dir, &paths.db_path, jsonl_path, cli, !ctx.is_json())
            .map_err(|err| {
                BeadsError::Config(format!(
                    "Repair import failed: {err}. \
             The JSONL file may be corrupt. \
             Try manually editing the JSONL to fix invalid records."
                ))
            })?;

    let post_repair = collect_doctor_report(&beads_dir, &paths)?;

    if ctx.is_json() {
        ctx.json(&serde_json::json!({
            "report": initial.report,
            "repaired": true,
            "local_repair": local_repair,
            "imported": repair_result.imported,
            "skipped": repair_result.skipped,
            "fk_violations_cleaned": repair_result.fk_violations_cleaned,
            "post_repair": post_repair.report,
            "verified": post_repair.report.ok,
        }));
    } else {
        ctx.info(&format!(
            "Repair complete: imported {}, skipped {}",
            repair_result.imported, repair_result.skipped
        ));
        ctx.info("Post-repair verification:");
        print_report(&post_repair.report, ctx)?;
    }

    if !post_repair.report.ok {
        return Err(BeadsError::Config(
            "Repair completed, but post-repair verification still found issues".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Issue, IssueType, Priority, Status};
    use crate::storage::SqliteStorage;
    use chrono::Utc;
    use fsqlite::Connection;
    use std::fs;
    use tempfile::{NamedTempFile, TempDir};

    fn find_check<'a>(checks: &'a [CheckResult], name: &str) -> Option<&'a CheckResult> {
        checks.iter().find(|check| check.name == name)
    }

    fn sample_issue(id: &str, title: &str) -> Issue {
        Issue {
            id: id.to_string(),
            content_hash: None,
            title: title.to_string(),
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            status: Status::Open,
            priority: Priority::MEDIUM,
            issue_type: IssueType::Task,
            assignee: None,
            owner: None,
            estimated_minutes: None,
            created_at: Utc::now(),
            created_by: None,
            updated_at: Utc::now(),
            closed_at: None,
            close_reason: None,
            closed_by_session: None,
            due_at: None,
            defer_until: None,
            external_ref: None,
            source_system: None,
            source_repo: None,
            deleted_at: None,
            deleted_by: None,
            delete_reason: None,
            original_type: None,
            compaction_level: None,
            compacted_at: None,
            compacted_at_commit: None,
            original_size: None,
            sender: None,
            ephemeral: false,
            pinned: false,
            is_template: false,
            labels: Vec::new(),
            dependencies: Vec::new(),
            comments: Vec::new(),
        }
    }

    #[test]
    fn test_check_jsonl_detects_malformed() -> Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(file.as_file_mut(), b"{\"id\":\"ok\"}\n")?;
        std::io::Write::write_all(file.as_file_mut(), b"{bad json}\n")?;

        let mut checks = Vec::new();
        let count = check_jsonl(file.path(), &mut checks).unwrap();
        assert_eq!(count, 2);

        let check = find_check(&checks, "jsonl.parse").expect("check present");
        assert!(matches!(check.status, CheckStatus::Error));

        Ok(())
    }

    #[test]
    fn test_required_schema_checks_missing_tables() {
        let conn = Connection::open(":memory:").unwrap();
        let mut checks = Vec::new();
        required_schema_checks(&conn, &mut checks).unwrap();

        let tables = find_check(&checks, "schema.tables").expect("tables check");
        assert!(matches!(tables.status, CheckStatus::Error));
    }

    #[test]
    fn test_collect_doctor_report_reports_missing_metadata_tables_without_aborting() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");

        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute(
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                priority INTEGER NOT NULL,
                issue_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            ",
        )
        .unwrap();
        fs::write(&jsonl_path, "{\"id\":\"bd-test\"}\n").unwrap();

        let paths = config::ConfigPaths {
            beads_dir: beads_dir.clone(),
            db_path,
            jsonl_path,
            metadata: config::Metadata::default(),
        };

        let report = collect_doctor_report(&beads_dir, &paths).expect("doctor report");
        let anomaly_check = find_check(&report.report.checks, "db.recoverable_anomalies")
            .expect("recoverable anomalies check");

        assert!(matches!(anomaly_check.status, CheckStatus::Error));
        assert!(
            anomaly_check
                .message
                .as_deref()
                .is_some_and(|message| message.contains("Failed to inspect recoverable anomalies")),
            "unexpected check message: {:?}",
            anomaly_check.message
        );
    }

    #[test]
    fn test_select_doctor_jsonl_path_keeps_missing_explicit_override() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let configured_jsonl = beads_dir.join("custom.jsonl");
        let legacy_jsonl = beads_dir.join("issues.jsonl");
        fs::write(&legacy_jsonl, "{\"id\":\"bd-legacy\"}\n").unwrap();

        let paths = config::ConfigPaths {
            beads_dir: beads_dir.clone(),
            db_path: beads_dir.join("beads.db"),
            jsonl_path: configured_jsonl.clone(),
            metadata: config::Metadata {
                database: "beads.db".to_string(),
                jsonl_export: "custom.jsonl".to_string(),
                backend: None,
                deletions_retention_days: None,
            },
        };

        assert_eq!(
            select_doctor_jsonl_path(&beads_dir, &paths),
            Some(configured_jsonl)
        );
    }

    #[test]
    fn test_collect_doctor_report_surfaces_missing_explicit_metadata_jsonl() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let db_path = beads_dir.join("beads.db");
        let configured_jsonl = beads_dir.join("custom.jsonl");
        fs::write(beads_dir.join("issues.jsonl"), "{\"id\":\"bd-legacy\"}\n").unwrap();
        let _storage = SqliteStorage::open(&db_path).unwrap();

        let paths = config::ConfigPaths {
            beads_dir: beads_dir.clone(),
            db_path,
            jsonl_path: configured_jsonl.clone(),
            metadata: config::Metadata {
                database: "beads.db".to_string(),
                jsonl_export: "custom.jsonl".to_string(),
                backend: None,
                deletions_retention_days: None,
            },
        };

        let report = collect_doctor_report(&beads_dir, &paths).expect("doctor report");
        let parse_check = find_check(&report.report.checks, "jsonl.parse").expect("jsonl parse");

        assert!(matches!(parse_check.status, CheckStatus::Error));
        assert_eq!(report.jsonl_path, Some(configured_jsonl.clone()));
        assert_eq!(
            parse_check
                .details
                .as_ref()
                .and_then(|details| details.get("path"))
                .and_then(serde_json::Value::as_str),
            Some(configured_jsonl.to_string_lossy().as_ref())
        );
    }

    #[test]
    fn test_collect_doctor_report_accepts_configured_external_jsonl() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let external_dir = temp.path().join("external");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::create_dir_all(&external_dir).unwrap();

        let db_path = beads_dir.join("beads.db");
        let external_jsonl = external_dir.join("issues.jsonl");
        fs::write(&external_jsonl, "{\"id\":\"bd-external\"}\n").unwrap();
        let _storage = SqliteStorage::open(&db_path).unwrap();

        let paths = config::ConfigPaths {
            beads_dir: beads_dir.clone(),
            db_path,
            jsonl_path: external_jsonl.clone(),
            metadata: config::Metadata {
                database: "beads.db".to_string(),
                jsonl_export: external_jsonl.to_string_lossy().into_owned(),
                backend: None,
                deletions_retention_days: None,
            },
        };

        let report = collect_doctor_report(&beads_dir, &paths).expect("doctor report");
        let sync_path_check =
            find_check(&report.report.checks, "sync_jsonl_path").expect("sync path check");

        assert!(matches!(sync_path_check.status, CheckStatus::Ok));
        assert_eq!(
            sync_path_check
                .details
                .as_ref()
                .and_then(|details| details.get("path"))
                .and_then(serde_json::Value::as_str),
            Some(external_jsonl.to_string_lossy().as_ref())
        );
        assert_eq!(
            sync_path_check
                .details
                .as_ref()
                .and_then(|details| details.get("external"))
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn test_integrity_check_messages_collects_all_rows() {
        let messages = integrity_check_messages(&[
            vec![SqliteValue::Text(
                "row 1 missing from index idx_a".to_string(),
            )],
            vec![SqliteValue::Text(
                "row 2 missing from index idx_a".to_string(),
            )],
        ]);

        assert_eq!(
            messages,
            vec![
                "row 1 missing from index idx_a".to_string(),
                "row 2 missing from index idx_a".to_string(),
            ]
        );
    }

    #[test]
    fn test_check_recoverable_anomalies_detects_duplicate_config_and_metadata() -> Result<()> {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("beads.db");
        let _storage = SqliteStorage::open(&db_path).unwrap();

        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        conn.execute("INSERT INTO config (key, value) VALUES ('issue_prefix', 'dup-a')")
            .unwrap();
        conn.execute("INSERT INTO config (key, value) VALUES ('issue_prefix', 'dup-b')")
            .unwrap();
        conn.execute("INSERT INTO metadata (key, value) VALUES ('project', 'dup-a')")
            .unwrap();
        conn.execute("INSERT INTO metadata (key, value) VALUES ('project', 'dup-b')")
            .unwrap();

        let mut checks = Vec::new();
        check_recoverable_anomalies(&conn, &mut checks)?;

        let check = find_check(&checks, "db.recoverable_anomalies").expect("check present");
        assert!(matches!(check.status, CheckStatus::Error));

        let findings = check
            .details
            .as_ref()
            .and_then(|details| details.get("findings"))
            .and_then(serde_json::Value::as_array)
            .expect("findings array");
        assert!(
            findings.iter().any(|finding| {
                finding
                    .as_str()
                    .is_some_and(|message| message.contains("config contains duplicate rows"))
            }),
            "expected duplicate config finding: {findings:?}"
        );
        assert!(
            findings.iter().any(|finding| {
                finding
                    .as_str()
                    .is_some_and(|message| message.contains("metadata contains duplicate rows"))
            }),
            "expected duplicate metadata finding: {findings:?}"
        );

        Ok(())
    }

    #[test]
    fn test_check_database_sidecars_detects_wal_without_shm() -> Result<()> {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("beads.db");
        fs::write(&db_path, b"sqlite-header-placeholder")?;
        fs::write(
            PathBuf::from(format!("{}-wal", db_path.to_string_lossy())),
            b"synthetic wal",
        )?;

        let mut checks = Vec::new();
        check_database_sidecars(&db_path, &mut checks)?;

        let check = find_check(&checks, "db.sidecars").expect("sidecar check");
        assert!(matches!(check.status, CheckStatus::Error));
        assert!(
            check.message.as_deref().is_some_and(|message| {
                message.contains("WAL sidecar exists without a matching SHM sidecar")
            }),
            "unexpected sidecar message: {:?}",
            check.message
        );
        Ok(())
    }

    #[test]
    fn test_check_recovery_artifacts_warns_on_preserved_database_family() -> Result<()> {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        fs::create_dir_all(&beads_dir)?;
        fs::write(beads_dir.join("beads.db.bad_20260312T000000Z"), b"backup")?;
        let recovery_dir = config::recovery_dir_for_db_path(&db_path, &beads_dir);
        fs::create_dir_all(&recovery_dir)?;
        fs::write(
            recovery_dir.join("beads.db.20260312T000000Z.rebuild-failed"),
            b"preserved",
        )?;

        let mut checks = Vec::new();
        check_recovery_artifacts(&beads_dir, &db_path, &mut checks)?;

        let check = find_check(&checks, "db.recovery_artifacts").expect("recovery artifact check");
        assert!(matches!(check.status, CheckStatus::Warn));
        let artifacts = check
            .details
            .as_ref()
            .and_then(|details| details.get("artifacts"))
            .and_then(serde_json::Value::as_array)
            .expect("artifact list");
        assert_eq!(artifacts.len(), 2);
        Ok(())
    }

    #[test]
    fn test_repair_recoverable_db_state_quarantines_orphan_shm_sidecar() -> Result<()> {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir)?;
        let db_path = beads_dir.join("beads.db");
        {
            let _storage = SqliteStorage::open(&db_path)?;
        }
        let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        let shm_path = PathBuf::from(format!("{}-shm", db_path.to_string_lossy()));
        let _ = fs::remove_file(&wal_path);
        let _ = fs::remove_file(&shm_path);
        fs::write(&shm_path, b"orphan shm")?;

        let report = DoctorReport {
            ok: false,
            checks: vec![CheckResult {
                name: "db.sidecars".to_string(),
                status: CheckStatus::Error,
                message: Some("SHM sidecar exists without a matching WAL sidecar".to_string()),
                details: None,
            }],
        };

        let repair = repair_recoverable_db_state(&beads_dir, &db_path, &report);
        assert!(
            !repair.quarantined_artifacts.is_empty(),
            "expected local repair to quarantine the orphan SHM sidecar"
        );

        let recovery_dir = config::recovery_dir_for_db_path(&db_path, &beads_dir);
        let backups: Vec<_> = fs::read_dir(&recovery_dir)?
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            backups
                .iter()
                .any(|name| name.starts_with("beads.db-shm.")
                    && name.ends_with(".doctor-quarantine")),
            "expected quarantined SHM backup in recovery dir: {backups:?}"
        );
        Ok(())
    }

    #[test]
    fn test_repair_recoverable_db_state_preserves_orphan_wal_when_checkpoint_fails() -> Result<()> {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir)?;
        let db_path = beads_dir.join("beads.db");
        fs::write(&db_path, b"not a sqlite database")?;
        let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        fs::write(&wal_path, b"orphan wal")?;

        let report = DoctorReport {
            ok: false,
            checks: vec![CheckResult {
                name: "db.sidecars".to_string(),
                status: CheckStatus::Error,
                message: Some("WAL sidecar exists without a matching SHM sidecar".to_string()),
                details: None,
            }],
        };

        let repair = repair_recoverable_db_state(&beads_dir, &db_path, &report);
        assert!(
            !repair.wal_checkpoint_completed,
            "checkpoint should not succeed against an invalid database"
        );
        assert!(
            repair.quarantined_artifacts.is_empty(),
            "orphan WAL should remain in place when checkpoint reconciliation fails"
        );
        assert!(
            wal_path.exists(),
            "orphan WAL should not be quarantined after failed reconciliation"
        );
        Ok(())
    }

    #[test]
    fn test_report_has_blocked_cache_stale_finding_detects_detail_entry() {
        let report = DoctorReport {
            ok: false,
            checks: vec![CheckResult {
                name: "db.recoverable_anomalies".to_string(),
                status: CheckStatus::Error,
                message: Some("config contains duplicate rows".to_string()),
                details: Some(serde_json::json!({
                    "findings": [
                        "config contains duplicate rows for key 'issue_prefix' (2 rows)",
                        BLOCKED_CACHE_STALE_FINDING,
                    ]
                })),
            }],
        };

        assert!(report_has_blocked_cache_stale_finding(&report));
    }

    #[test]
    fn test_report_has_blocked_cache_stale_finding_ignores_other_recoverable_errors() {
        let report = DoctorReport {
            ok: false,
            checks: vec![CheckResult {
                name: "db.recoverable_anomalies".to_string(),
                status: CheckStatus::Error,
                message: Some("config contains duplicate rows".to_string()),
                details: Some(serde_json::json!({
                    "findings": [
                        "config contains duplicate rows for key 'issue_prefix' (2 rows)",
                        "metadata contains duplicate rows for key 'project' (2 rows)",
                    ]
                })),
            }],
        };

        assert!(!report_has_blocked_cache_stale_finding(&report));
    }

    #[test]
    fn test_check_issue_write_probe_succeeds_on_healthy_database() {
        let temp = TempDir::new().unwrap();
        let db_path = temp.path().join("beads.db");

        {
            let mut storage = SqliteStorage::open(&db_path).unwrap();
            storage
                .create_issue(&sample_issue("bd-probe", "Probe me"), "tester")
                .unwrap();
        }

        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        let mut checks = Vec::new();
        check_issue_write_probe(&conn, &mut checks);

        let check = find_check(&checks, "db.write_probe").expect("check present");
        assert!(matches!(check.status, CheckStatus::Ok));
        assert!(
            check
                .message
                .as_deref()
                .is_some_and(|message| message.contains("bd-probe")),
            "unexpected check message: {:?}",
            check.message
        );
    }

    #[test]
    fn test_inspect_existing_doctor_database_uses_live_write_probe() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();
        let db_path = beads_dir.join("beads.db");

        {
            let mut storage = SqliteStorage::open(&db_path).unwrap();
            storage
                .create_issue(&sample_issue("bd-probe", "Probe me"), "tester")
                .unwrap();
        }

        let lock_conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
        lock_conn.execute("BEGIN IMMEDIATE").unwrap();

        let mut checks = Vec::new();
        inspect_existing_doctor_database(&db_path, None, None, &mut checks);

        let check = find_check(&checks, "db.write_probe").expect("check present");
        assert!(
            matches!(check.status, CheckStatus::Warn),
            "unexpected live write probe status: {:?}",
            check.status
        );
        assert!(
            check.message.as_deref().is_some_and(|message| {
                message.contains("Failed to begin rollback-only write probe")
            }),
            "unexpected check message: {:?}",
            check.message
        );

        lock_conn.execute("ROLLBACK").unwrap();
    }

    #[test]
    fn test_build_issue_write_probe_check_marks_rollback_failure_as_error() {
        let check = build_issue_write_probe_check(
            "bd-probe",
            Ok(1),
            Err(FrankenError::Internal("rollback failed".to_string())),
        );

        assert!(matches!(check.status, CheckStatus::Error));
        assert!(
            check
                .message
                .as_deref()
                .is_some_and(|message| message.contains("rollback failed")),
            "unexpected check message: {:?}",
            check.message
        );
        assert_eq!(check.details.unwrap()["issue_id"], "bd-probe");
    }

    #[test]
    fn test_build_issue_write_probe_check_preserves_write_failure() {
        let check = build_issue_write_probe_check(
            "bd-probe",
            Err(FrankenError::Internal("write failed".to_string())),
            Ok(0),
        );

        assert!(matches!(check.status, CheckStatus::Error));
        assert!(
            check
                .message
                .as_deref()
                .is_some_and(|message| message.contains("write failed")),
            "unexpected check message: {:?}",
            check.message
        );
    }

    #[test]
    fn test_repair_database_from_jsonl_restores_original_db_on_import_failure() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");

        {
            let mut storage = SqliteStorage::open(&db_path).unwrap();
            let issue = Issue {
                id: "bd-keep".to_string(),
                content_hash: None,
                title: "Keep me".to_string(),
                description: None,
                design: None,
                acceptance_criteria: None,
                notes: None,
                status: Status::Open,
                priority: Priority::MEDIUM,
                issue_type: IssueType::Task,
                assignee: None,
                owner: None,
                estimated_minutes: None,
                created_at: Utc::now(),
                created_by: None,
                updated_at: Utc::now(),
                closed_at: None,
                close_reason: None,
                closed_by_session: None,
                due_at: None,
                defer_until: None,
                external_ref: None,
                source_system: None,
                source_repo: None,
                deleted_at: None,
                deleted_by: None,
                delete_reason: None,
                original_type: None,
                compaction_level: None,
                compacted_at: None,
                compacted_at_commit: None,
                original_size: None,
                sender: None,
                ephemeral: false,
                pinned: false,
                is_template: false,
                labels: Vec::new(),
                dependencies: Vec::new(),
                comments: Vec::new(),
            };
            storage.create_issue(&issue, "tester").unwrap();
        }

        fs::write(&jsonl_path, "not valid json\n").unwrap();

        let err = repair_database_from_jsonl(
            &beads_dir,
            &db_path,
            &jsonl_path,
            &config::CliOverrides::default(),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Invalid JSON"),
            "unexpected error: {err}"
        );

        let reopened = SqliteStorage::open(&db_path).unwrap();
        let issue = reopened
            .get_issue("bd-keep")
            .unwrap()
            .expect("original DB should be restored after failed repair");
        assert_eq!(issue.title, "Keep me");

        let recovery_dir = beads_dir.join(".br_recovery");
        let backup_count =
            fs::read_dir(&recovery_dir).map_or(0, |entries| entries.flatten().count());
        assert_eq!(
            backup_count, 0,
            "preflight failures should not create recovery backups"
        );
    }

    #[test]
    fn test_repair_database_from_jsonl_restores_issue_prefix_from_jsonl() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");

        let issue = Issue {
            id: "proj-abc123".to_string(),
            content_hash: None,
            title: "Imported".to_string(),
            description: None,
            design: None,
            acceptance_criteria: None,
            notes: None,
            status: Status::Open,
            priority: Priority::MEDIUM,
            issue_type: IssueType::Task,
            assignee: None,
            owner: None,
            estimated_minutes: None,
            created_at: Utc::now(),
            created_by: None,
            updated_at: Utc::now(),
            closed_at: None,
            close_reason: None,
            closed_by_session: None,
            due_at: None,
            defer_until: None,
            external_ref: None,
            source_system: None,
            source_repo: None,
            deleted_at: None,
            deleted_by: None,
            delete_reason: None,
            original_type: None,
            compaction_level: None,
            compacted_at: None,
            compacted_at_commit: None,
            original_size: None,
            sender: None,
            ephemeral: false,
            pinned: false,
            is_template: false,
            labels: Vec::new(),
            dependencies: Vec::new(),
            comments: Vec::new(),
        };
        fs::write(
            &jsonl_path,
            format!("{}\n", serde_json::to_string(&issue).unwrap()),
        )
        .unwrap();

        let result = repair_database_from_jsonl(
            &beads_dir,
            &db_path,
            &jsonl_path,
            &config::CliOverrides::default(),
            false,
        )
        .unwrap();

        assert_eq!(result.imported, 1);

        let reopened = SqliteStorage::open(&db_path).unwrap();
        assert_eq!(
            reopened.get_config("issue_prefix").unwrap().as_deref(),
            Some("proj")
        );
    }

    #[test]
    fn test_repair_recoverable_db_state_skips_missing_db() {
        let temp = TempDir::new().unwrap();
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();
        let db_path = temp.path().join("missing.db");
        let report = DoctorReport {
            ok: false,
            checks: Vec::new(),
        };

        let local_repair = repair_recoverable_db_state(&beads_dir, &db_path, &report);
        assert!(!local_repair.blocked_cache_rebuilt);
    }
}
