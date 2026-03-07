//! Configuration management for `beads_rust`.
//!
//! Configuration sources and precedence (highest wins):
//! 1. CLI overrides
//! 2. Environment variables
//! 3. Project config (.beads/config.yaml)
//! 4. User config (~/.config/beads/config.yaml; falls back to ~/.config/bd/config.yaml)
//! 5. Legacy user config (~/.beads/config.yaml)
//! 6. DB config table
//! 7. Defaults

pub mod routing;

use crate::error::{BeadsError, Result};
use crate::model::{IssueType, Priority};
use crate::storage::SqliteStorage;
use crate::sync::{
    ExportConfig, ImportConfig, ImportResult, export_to_jsonl_with_policy, finalize_export,
    import_from_jsonl, preflight_import,
};
use crate::util::id::IdConfig;
use chrono::Utc;
use fsqlite_error::FrankenError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, IsTerminal};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::warn;

/// Default database filename used when metadata is missing.
const DEFAULT_DB_FILENAME: &str = "beads.db";
/// Default JSONL filename used when metadata is missing.
const DEFAULT_JSONL_FILENAME: &str = "issues.jsonl";
/// Legacy JSONL filename to fall back to.
const LEGACY_JSONL_FILENAME: &str = "beads.jsonl";
/// Directory used for automatic database recovery backups.
const RECOVERY_DIR_NAME: &str = ".br_recovery";

/// JSONL files that should never be treated as the main export file.
/// Includes merge artifacts, deletion logs, and interaction logs.
const EXCLUDED_JSONL_FILES: &[&str] = &[
    "deletions.jsonl",
    "interactions.jsonl",
    "beads.base.jsonl",
    "beads.left.jsonl",
    "beads.right.jsonl",
    "sync_base.jsonl",
];

/// Startup metadata describing DB + JSONL paths.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Metadata {
    pub database: String,
    pub jsonl_export: String,
    #[serde(default)]
    pub backend: Option<String>,
    #[serde(default)]
    pub deletions_retention_days: Option<u64>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            database: DEFAULT_DB_FILENAME.to_string(),
            jsonl_export: DEFAULT_JSONL_FILENAME.to_string(),
            backend: None,
            deletions_retention_days: None,
        }
    }
}

impl Metadata {
    /// Load metadata.json from the beads directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn load(beads_dir: &Path) -> Result<Self> {
        let path = beads_dir.join("metadata.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = fs::read_to_string(&path)?;
        let mut metadata: Self = serde_json::from_str(&contents)?;

        if metadata.database.trim().is_empty() {
            metadata.database = DEFAULT_DB_FILENAME.to_string();
        }
        if metadata.jsonl_export.trim().is_empty() {
            metadata.jsonl_export = DEFAULT_JSONL_FILENAME.to_string();
        }

        Ok(metadata)
    }
}

/// Discover the best JSONL file in the beads directory.
///
/// Selection rules:
/// 1. Prefer `issues.jsonl` if present.
/// 2. Fall back to `beads.jsonl` (legacy) if present.
/// 3. Never use merge artifacts (`beads.base.jsonl`, `beads.left.jsonl`, `beads.right.jsonl`).
/// 4. Never use deletion logs (`deletions.jsonl`) or interaction logs (`interactions.jsonl`).
/// 5. If no valid JSONL exists, return `None` (caller should use default for writing).
#[must_use]
pub fn discover_jsonl(beads_dir: &Path) -> Option<PathBuf> {
    // Check preferred file first
    let issues_path = beads_dir.join(DEFAULT_JSONL_FILENAME);
    if issues_path.is_file() {
        return Some(issues_path);
    }

    // Check legacy file
    let legacy_path = beads_dir.join(LEGACY_JSONL_FILENAME);
    if legacy_path.is_file() {
        return Some(legacy_path);
    }

    // No valid JSONL found
    None
}

/// Check if a JSONL filename should be excluded from discovery.
///
/// Returns `true` for merge artifacts, deletion logs, and interaction logs.
#[must_use]
pub fn is_excluded_jsonl(filename: &str) -> bool {
    EXCLUDED_JSONL_FILES.contains(&filename)
}

/// Resolved paths for this workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigPaths {
    pub beads_dir: PathBuf,
    pub db_path: PathBuf,
    pub jsonl_path: PathBuf,
    pub metadata: Metadata,
}

impl ConfigPaths {
    /// Resolve database + JSONL paths using metadata and environment overrides.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read.
    pub fn resolve(beads_dir: &Path, db_override: Option<&PathBuf>) -> Result<Self> {
        let metadata = Metadata::load(beads_dir)?;
        let db_path = resolve_db_path(beads_dir, &metadata, db_override);
        let jsonl_path = resolve_jsonl_path(beads_dir, &metadata, db_override);

        Ok(Self {
            beads_dir: beads_dir.to_path_buf(),
            db_path,
            jsonl_path,
            metadata,
        })
    }

    /// Get the user config path (~/.config/beads/config.yaml).
    /// Returns None if HOME is not set.
    #[must_use]
    pub fn user_config_path(&self) -> Option<PathBuf> {
        env::var("HOME").ok().map(|home| {
            let config_root = Path::new(&home).join(".config");
            let beads_path = config_root.join("beads").join("config.yaml");
            if beads_path.exists() {
                beads_path
            } else {
                config_root.join("bd").join("config.yaml")
            }
        })
    }

    /// Get the legacy user config path (~/.beads/config.yaml).
    /// Returns None if HOME is not set.
    #[must_use]
    pub fn legacy_user_config_path(&self) -> Option<PathBuf> {
        env::var("HOME")
            .ok()
            .map(|home| Path::new(&home).join(".beads").join("config.yaml"))
    }

    /// Get the project config path (.beads/config.yaml).
    #[must_use]
    pub fn project_config_path(&self) -> Option<PathBuf> {
        Some(self.beads_dir.join("config.yaml"))
    }
}

/// Discover the active `.beads` directory.
///
/// Honors `BEADS_DIR` when set, otherwise walks up from `start` (or CWD).
///
/// # Errors
///
/// Returns an error if no beads directory is found or the CWD cannot be read.
pub fn discover_beads_dir(start: Option<&Path>) -> Result<PathBuf> {
    discover_beads_dir_with_env(start, None)
}

fn discover_beads_dir_with_env(
    start: Option<&Path>,
    env_override: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = env_override {
        if path.is_dir() {
            return routing::follow_redirects(path, 10);
        }
    } else if let Ok(value) = env::var("BEADS_DIR")
        && !value.trim().is_empty()
    {
        let path = PathBuf::from(value);
        if path.is_dir() {
            return routing::follow_redirects(&path, 10);
        }
    }

    let mut current = match start {
        Some(path) => path.to_path_buf(),
        None => env::current_dir()?,
    };

    loop {
        let candidate = current.join(".beads");
        if candidate.is_dir() {
            return routing::follow_redirects(&candidate, 10);
        }

        if !current.pop() {
            break;
        }
    }

    Err(BeadsError::NotInitialized)
}

/// Discover beads directory, using `--db` path if provided.
///
/// When `--db` is explicitly provided, derives the beads_dir from that path
/// (e.g., `/path/to/.beads/beads.db` → `/path/to/.beads/`), allowing br to work
/// from any directory. Falls back to normal discovery when `--db` is not set.
///
/// # Errors
///
/// Returns an error if:
/// - `--db` path doesn't contain `.beads/` component
/// - No beads directory found (when `--db` not provided)
pub fn discover_beads_dir_with_cli(cli: &CliOverrides) -> Result<PathBuf> {
    cli.db.as_ref().map_or_else(
        // Fall back to normal discovery when --db is not set
        || discover_beads_dir(None),
        // Derive beads_dir from explicit --db path
        |db_path| derive_beads_dir_from_db_path(db_path),
    )
}

/// Discover the active `.beads` directory, but allow "no workspace" when no
/// explicit `--db` target was provided.
///
/// This is intended for commands that can operate outside a project and should
/// only suppress `NotInitialized` when the user did not explicitly point to a
/// database.
///
/// # Errors
///
/// Returns an error when:
/// - An explicit `--db` path is invalid
/// - Discovery fails for reasons other than `NotInitialized`
pub fn discover_optional_beads_dir_with_cli(cli: &CliOverrides) -> Result<Option<PathBuf>> {
    match discover_beads_dir_with_cli(cli) {
        Ok(path) => Ok(Some(path)),
        Err(BeadsError::NotInitialized) if cli.db.is_none() => Ok(None),
        Err(err) => Err(err),
    }
}

/// Extract the `.beads/` directory from a database path.
///
/// E.g., `/path/to/.beads/beads.db` → `/path/to/.beads/`
fn derive_beads_dir_from_db_path(db_path: &Path) -> Result<PathBuf> {
    // Walk up the path looking for a component named ".beads"
    let mut current = db_path.to_path_buf();

    // If the path points to a file, start from its parent
    if current.is_file() {
        current.pop();
    }

    // Check if current directory is .beads
    if current.file_name().is_some_and(|n| n == ".beads") {
        return Ok(current);
    }

    // Walk up looking for .beads
    for ancestor in db_path.ancestors() {
        if ancestor.file_name().is_some_and(|n| n == ".beads") {
            return Ok(ancestor.to_path_buf());
        }
    }

    Err(BeadsError::validation(
        "db",
        format!(
            "Cannot derive beads directory from path '{}': expected path to contain '.beads/' component",
            db_path.display()
        ),
    ))
}

#[derive(Debug)]
struct RecoveryBackupSet {
    db_path: PathBuf,
    recovery_dir: PathBuf,
    stamp: String,
    files: Vec<(PathBuf, PathBuf)>,
}

fn open_sqlite_storage_with_recovery(
    beads_dir: &Path,
    paths: &ConfigPaths,
    lock_timeout: Option<u64>,
    bootstrap_layer: &ConfigLayer,
) -> Result<SqliteStorage> {
    match SqliteStorage::open_with_timeout(&paths.db_path, lock_timeout) {
        Ok(storage) => Ok(storage),
        Err(open_err) => {
            if !should_attempt_jsonl_recovery(&open_err, &paths.db_path, &paths.jsonl_path) {
                return Err(open_err);
            }

            match rebuild_database_from_jsonl(beads_dir, paths, lock_timeout, bootstrap_layer) {
                Ok(storage) => Ok(storage),
                Err(recovery_err) => {
                    warn!(
                        db_path = %paths.db_path.display(),
                        jsonl_path = %paths.jsonl_path.display(),
                        open_error = %open_err,
                        recovery_error = %recovery_err,
                        "Automatic database recovery from JSONL failed"
                    );
                    if should_surface_recovery_error(&recovery_err) {
                        Err(recovery_err)
                    } else {
                        Err(open_err)
                    }
                }
            }
        }
    }
}

fn should_attempt_jsonl_recovery(open_err: &BeadsError, db_path: &Path, jsonl_path: &Path) -> bool {
    if !db_path.is_file() || !jsonl_path.is_file() {
        return false;
    }

    matches!(
        open_err,
        BeadsError::Database(
            FrankenError::DatabaseCorrupt { .. }
                | FrankenError::NotADatabase { .. }
                | FrankenError::WalCorrupt { .. }
                | FrankenError::ShortRead { .. }
        )
    )
}

fn rebuild_database_from_jsonl(
    beads_dir: &Path,
    paths: &ConfigPaths,
    lock_timeout: Option<u64>,
    bootstrap_layer: &ConfigLayer,
) -> Result<SqliteStorage> {
    repair_database_from_jsonl(
        beads_dir,
        &paths.db_path,
        &paths.jsonl_path,
        lock_timeout,
        bootstrap_layer,
        false,
    )
    .map(|(storage, _)| storage)
}

pub(crate) fn repair_database_from_jsonl(
    beads_dir: &Path,
    db_path: &Path,
    jsonl_path: &Path,
    lock_timeout: Option<u64>,
    bootstrap_layer: &ConfigLayer,
    show_progress: bool,
) -> Result<(SqliteStorage, ImportResult)> {
    let prefix = resolve_bootstrap_issue_prefix(bootstrap_layer, beads_dir, jsonl_path)?;
    let mut import_config = import_config_for_resolved_jsonl(beads_dir, jsonl_path);
    import_config.show_progress = show_progress;

    preflight_import(jsonl_path, &import_config, Some(&prefix))?.into_result()?;

    warn!(
        db_path = %db_path.display(),
        jsonl_path = %jsonl_path.display(),
        "Rebuilding SQLite database from JSONL"
    );

    let ((storage, import_result), recovery_dir) =
        rebuild_database_family_with_backup(db_path, beads_dir, || {
            rebuild_database_family(db_path, lock_timeout, jsonl_path, &import_config, &prefix)
        })?;

    warn!(
        db_path = %db_path.display(),
        recovery_dir = %recovery_dir.display(),
        "SQLite rebuild from JSONL succeeded"
    );
    Ok((storage, import_result))
}

fn should_surface_recovery_error(recovery_err: &BeadsError) -> bool {
    matches!(recovery_err, BeadsError::WithContext { .. })
}

fn recovery_restore_failure(
    backup_set: &RecoveryBackupSet,
    recovery_err: &BeadsError,
    restore_err: BeadsError,
) -> BeadsError {
    BeadsError::WithContext {
        context: format!(
            "Automatic database recovery failed ({recovery_err}); original database restore from '{}' also failed",
            backup_set.recovery_dir.display()
        ),
        source: Box::new(restore_err),
    }
}

fn rebuild_database_family(
    db_path: &Path,
    lock_timeout: Option<u64>,
    jsonl_path: &Path,
    import_config: &ImportConfig,
    prefix: &str,
) -> Result<(SqliteStorage, ImportResult)> {
    let mut storage = SqliteStorage::open_with_timeout(db_path, lock_timeout)?;
    storage.set_config("issue_prefix", prefix)?;
    let import_result = import_from_jsonl(&mut storage, jsonl_path, import_config, Some(prefix))?;
    Ok((storage, import_result))
}

pub(crate) fn rebuild_database_family_with_backup<T, F>(
    db_path: &Path,
    beads_dir: &Path,
    rebuild: F,
) -> Result<(T, PathBuf)>
where
    F: FnOnce() -> Result<T>,
{
    let backup_set = backup_database_family_for_recovery(db_path, beads_dir)?;
    let recovery_dir = backup_set.recovery_dir.clone();

    match rebuild() {
        Ok(value) => Ok((value, recovery_dir)),
        Err(rebuild_err) => {
            if let Err(restore_err) = restore_database_family_after_failed_rebuild(&backup_set) {
                warn!(
                    db_path = %db_path.display(),
                    recovery_dir = %backup_set.recovery_dir.display(),
                    restore_error = %restore_err,
                    "Failed to restore original database after unsuccessful rebuild"
                );
                return Err(recovery_restore_failure(
                    &backup_set,
                    &rebuild_err,
                    restore_err,
                ));
            }
            Err(rebuild_err)
        }
    }
}

fn backup_database_family_for_recovery(
    db_path: &Path,
    beads_dir: &Path,
) -> Result<RecoveryBackupSet> {
    let stamp = Utc::now().format("%Y%m%d_%H%M%S_%f").to_string();
    move_database_family_to_recovery(db_path, beads_dir, &stamp)
}

fn move_database_family_to_recovery(
    db_path: &Path,
    beads_dir: &Path,
    stamp: &str,
) -> Result<RecoveryBackupSet> {
    let recovery_dir = recovery_dir_for_db_path(db_path, beads_dir);
    fs::create_dir_all(&recovery_dir)?;

    let mut files: Vec<(PathBuf, PathBuf)> = Vec::new();

    for original in database_family_paths(db_path) {
        if !original.exists() {
            continue;
        }

        let backup = recovery_dir.join(recovery_backup_filename(&original, stamp, "bak"));
        if let Err(err) = fs::rename(&original, &backup) {
            for (restored_original, restored_backup) in files.iter().rev() {
                if let Err(rollback_err) = fs::rename(restored_backup, restored_original) {
                    warn!(
                        original = %restored_original.display(),
                        backup = %restored_backup.display(),
                        rollback_error = %rollback_err,
                        "Failed to roll back partially completed recovery backup"
                    );
                }
            }
            return Err(err.into());
        }
        files.push((original, backup));
    }

    Ok(RecoveryBackupSet {
        db_path: db_path.to_path_buf(),
        recovery_dir,
        stamp: stamp.to_string(),
        files,
    })
}

fn restore_database_family_after_failed_rebuild(backup_set: &RecoveryBackupSet) -> Result<()> {
    for rebuilt in database_family_paths(&backup_set.db_path) {
        if !rebuilt.exists() {
            continue;
        }

        let failed_backup = backup_set.recovery_dir.join(recovery_backup_filename(
            &rebuilt,
            &backup_set.stamp,
            "rebuild-failed",
        ));
        fs::rename(&rebuilt, &failed_backup)?;
    }

    for (original, backup) in &backup_set.files {
        if backup.exists() {
            fs::rename(backup, original)?;
        }
    }

    Ok(())
}

fn recovery_dir_for_db_path(db_path: &Path, beads_dir: &Path) -> PathBuf {
    db_path
        .parent()
        .unwrap_or(beads_dir)
        .join(RECOVERY_DIR_NAME)
}

fn database_family_paths(db_path: &Path) -> Vec<PathBuf> {
    let db_string = db_path.to_string_lossy();
    vec![
        db_path.to_path_buf(),
        PathBuf::from(format!("{db_string}-wal")),
        PathBuf::from(format!("{db_string}-shm")),
    ]
}

fn recovery_backup_filename(path: &Path, stamp: &str, suffix: &str) -> String {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("beads.db");
    format!("{filename}.{stamp}.{suffix}")
}

/// Open storage using resolved config paths, returning the storage and paths used.
///
/// # Errors
///
/// Returns an error if metadata cannot be read or the database cannot be opened.
pub fn open_storage(
    beads_dir: &Path,
    db_override: Option<&PathBuf>,
    lock_timeout: Option<u64>,
) -> Result<(SqliteStorage, ConfigPaths)> {
    let startup_layer = load_startup_config(beads_dir)?;
    let resolved_db_override = db_override
        .cloned()
        .or_else(|| db_override_from_layer(&startup_layer));
    let resolved_lock_timeout = lock_timeout
        .or_else(|| lock_timeout_from_layer(&startup_layer))
        .or(Some(30000));
    let paths = ConfigPaths::resolve(beads_dir, resolved_db_override.as_ref())?;
    let storage = open_sqlite_storage_with_recovery(
        beads_dir,
        &paths,
        resolved_lock_timeout,
        &startup_layer,
    )?;
    Ok((storage, paths))
}

/// Storage handle with no-db awareness.
#[derive(Debug)]
pub struct OpenStorageResult {
    pub storage: SqliteStorage,
    pub paths: ConfigPaths,
    pub no_db: bool,
}

impl OpenStorageResult {
    /// Flush JSONL if no-db mode is enabled and there are dirty issues.
    ///
    /// # Errors
    ///
    /// Returns an error if JSONL export fails.
    pub fn flush_no_db_if_dirty(&mut self) -> Result<()> {
        if !self.no_db {
            return Ok(());
        }

        if self.storage.get_dirty_issue_count()? == 0 {
            return Ok(());
        }

        let export_config = ExportConfig {
            force: false,
            is_default_path: self.paths.jsonl_path == self.paths.beads_dir.join("issues.jsonl"),
            beads_dir: Some(self.paths.beads_dir.clone()),
            allow_external_jsonl: resolved_jsonl_path_is_external(
                &self.paths.beads_dir,
                &self.paths.jsonl_path,
            ),
            show_progress: false,
            ..Default::default()
        };

        let (export_result, _report) =
            export_to_jsonl_with_policy(&self.storage, &self.paths.jsonl_path, &export_config)?;
        finalize_export(
            &mut self.storage,
            &export_result,
            Some(&export_result.issue_hashes),
        )?;

        Ok(())
    }
}

/// Open storage with CLI overrides and support for `--no-db` mode.
///
/// # Errors
///
/// Returns an error if configuration loading, JSONL import, or storage setup fails.
pub fn open_storage_with_cli(beads_dir: &Path, cli: &CliOverrides) -> Result<OpenStorageResult> {
    let startup_layer = load_startup_config(beads_dir)?;
    let cli_layer = cli.as_layer();
    let merged_layer = ConfigLayer::merge_layers(&[startup_layer, cli_layer]);

    let no_db = no_db_from_layer(&merged_layer).unwrap_or(false);

    let resolved_db_override = cli
        .db
        .clone()
        .or_else(|| db_override_from_layer(&merged_layer));
    let resolved_lock_timeout = cli
        .lock_timeout
        .or_else(|| lock_timeout_from_layer(&merged_layer))
        .or(Some(30000));

    let paths = ConfigPaths::resolve(beads_dir, resolved_db_override.as_ref())?;

    if no_db {
        let mut storage = SqliteStorage::open_memory()?;
        let prefix = resolve_bootstrap_issue_prefix(&merged_layer, beads_dir, &paths.jsonl_path)?;
        storage.set_config("issue_prefix", &prefix)?;

        if paths.jsonl_path.is_file() {
            let import_config = import_config_for_resolved_jsonl(beads_dir, &paths.jsonl_path);
            import_from_jsonl(
                &mut storage,
                &paths.jsonl_path,
                &import_config,
                Some(&prefix),
            )?;
        }

        Ok(OpenStorageResult {
            storage,
            paths,
            no_db,
        })
    } else {
        let storage = open_sqlite_storage_with_recovery(
            beads_dir,
            &paths,
            resolved_lock_timeout,
            &merged_layer,
        )?;
        Ok(OpenStorageResult {
            storage,
            paths,
            no_db,
        })
    }
}

#[must_use]
pub fn no_db_from_layer(layer: &ConfigLayer) -> Option<bool> {
    get_startup_value(layer, &["no-db", "no_db", "no.db"]).and_then(|value| parse_bool(value))
}

/// Check merged config for `no-auto-flush` / `sync.auto_flush` (inverted).
#[must_use]
pub fn no_auto_flush_from_layer(layer: &ConfigLayer) -> Option<bool> {
    // Direct key: no-auto-flush / no_auto_flush / no.auto.flush
    if let Some(v) = get_startup_value(layer, &["no-auto-flush", "no_auto_flush", "no.auto.flush"])
        .and_then(|value| parse_bool(value))
    {
        return Some(v);
    }
    // Inverted key: sync.auto_flush (false => no_auto_flush = true)
    get_startup_value(layer, &["sync.auto_flush", "sync.auto-flush"])
        .and_then(|value| parse_bool(value))
        .map(|v| !v)
}

/// Check merged config for `no-auto-import` / `sync.auto_import` (inverted).
#[must_use]
pub fn no_auto_import_from_layer(layer: &ConfigLayer) -> Option<bool> {
    // Direct key: no-auto-import / no_auto_import / no.auto.import
    if let Some(v) = get_startup_value(
        layer,
        &["no-auto-import", "no_auto_import", "no.auto.import"],
    )
    .and_then(|value| parse_bool(value))
    {
        return Some(v);
    }
    // Inverted key: sync.auto_import (false => no_auto_import = true)
    get_startup_value(layer, &["sync.auto_import", "sync.auto-import"])
        .and_then(|value| parse_bool(value))
        .map(|v| !v)
}

fn resolve_bootstrap_issue_prefix(
    bootstrap_layer: &ConfigLayer,
    beads_dir: &Path,
    jsonl_path: &Path,
) -> Result<String> {
    if let Some(prefix) = get_value(bootstrap_layer, &["issue_prefix", "issue-prefix", "prefix"]) {
        let trimmed = prefix.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Some(prefix) = common_prefix_from_jsonl(jsonl_path)? {
        return Ok(prefix);
    }

    if let Some(name) = beads_dir
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        return Ok(name.to_string());
    }

    Ok("bd".to_string())
}

fn import_config_for_resolved_jsonl(beads_dir: &Path, jsonl_path: &Path) -> ImportConfig {
    ImportConfig {
        beads_dir: Some(beads_dir.to_path_buf()),
        allow_external_jsonl: resolved_jsonl_path_is_external(beads_dir, jsonl_path),
        show_progress: false,
        ..Default::default()
    }
}

fn resolved_jsonl_path_is_external(beads_dir: &Path, jsonl_path: &Path) -> bool {
    !path_is_within_beads_dir(jsonl_path, beads_dir)
}

fn path_is_within_beads_dir(path: &Path, beads_dir: &Path) -> bool {
    let canonical_beads =
        dunce::canonicalize(beads_dir).unwrap_or_else(|_| beads_dir.to_path_buf());

    let effective_path = if path.exists() {
        dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    } else if let Some(parent) = path.parent().filter(|parent| parent.exists()) {
        let canonical_parent = dunce::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
        path.file_name().map_or_else(
            || canonical_parent.clone(),
            |name| canonical_parent.join(name),
        )
    } else {
        path.to_path_buf()
    };

    effective_path.starts_with(beads_dir) || effective_path.starts_with(&canonical_beads)
}

/// Fast prefix inference: reads only the first issue from JSONL.
/// Used by `load_config` on every command — must be O(1) not O(n).
fn first_prefix_from_jsonl(jsonl_path: &Path) -> Result<Option<String>> {
    if !jsonl_path.is_file() {
        return Ok(None);
    }

    let file = std::fs::File::open(jsonl_path)?;
    let reader = std::io::BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(id) = value.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some((prefix, _)) = id.split_once('-') else {
            continue;
        };
        if !prefix.is_empty() {
            return Ok(Some(prefix.to_string()));
        }
    }

    Ok(None)
}

fn common_prefix_from_jsonl(jsonl_path: &Path) -> Result<Option<String>> {
    if !jsonl_path.is_file() {
        return Ok(None);
    }

    let file = std::fs::File::open(jsonl_path)?;
    let reader = std::io::BufReader::new(file);
    let mut prefixes: HashSet<String> = HashSet::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            BeadsError::Config(format!("Invalid JSON at line {}: {}", line_num + 1, e))
        })?;
        let Some(id) = value.get("id").and_then(|v| v.as_str()) else {
            continue;
        };

        let Some((prefix, _)) = id.split_once('-') else {
            return Err(BeadsError::InvalidId { id: id.to_string() });
        };
        if prefix.is_empty() {
            return Err(BeadsError::InvalidId { id: id.to_string() });
        }

        prefixes.insert(prefix.to_string());
        if prefixes.len() > 1 {
            return Err(BeadsError::Config(
                "Mixed issue prefixes detected in JSONL. Set issue-prefix in .beads/config.yaml."
                    .to_string(),
            ));
        }
    }

    Ok(prefixes.into_iter().next())
}

/// Resolve config paths using startup config layers for overrides.
///
/// # Errors
///
/// Returns an error if startup config cannot be read or metadata cannot be loaded.
pub fn resolve_paths(beads_dir: &Path, db_override: Option<&PathBuf>) -> Result<ConfigPaths> {
    let startup_layer = load_startup_config(beads_dir)?;
    let resolved_db_override = db_override
        .cloned()
        .or_else(|| db_override_from_layer(&startup_layer));
    ConfigPaths::resolve(beads_dir, resolved_db_override.as_ref())
}

fn resolve_db_path(
    beads_dir: &Path,
    metadata: &Metadata,
    db_override: Option<&PathBuf>,
) -> PathBuf {
    if let Some(override_path) = db_override {
        return override_path.clone();
    }

    let candidate = PathBuf::from(&metadata.database);
    if candidate.is_absolute() {
        candidate
    } else {
        // Use BEADS_CACHE_DIR if set, otherwise beads_dir
        // This allows storing the database on a fast local filesystem
        // when .beads is on a slow network mount
        crate::util::resolve_cache_dir(beads_dir).join(candidate)
    }
}

fn resolve_jsonl_path(
    beads_dir: &Path,
    metadata: &Metadata,
    db_override: Option<&PathBuf>,
) -> PathBuf {
    // Priority 1: BEADS_JSONL environment variable (highest priority)
    if let Ok(env_path) = env::var("BEADS_JSONL")
        && !env_path.trim().is_empty()
    {
        return PathBuf::from(env_path);
    }

    // Priority 2: DB override derives sibling JSONL path
    if db_override.is_some() {
        return db_override
            .and_then(|path| {
                path.parent()
                    .map(|parent| parent.join(DEFAULT_JSONL_FILENAME))
            })
            .unwrap_or_else(|| beads_dir.join(DEFAULT_JSONL_FILENAME));
    }

    // Priority 3: metadata.json override (if explicitly set to non-default)
    let metadata_jsonl = &metadata.jsonl_export;
    let is_explicit_override =
        metadata_jsonl != DEFAULT_JSONL_FILENAME && !is_excluded_jsonl(metadata_jsonl);

    if is_explicit_override {
        let candidate = PathBuf::from(metadata_jsonl);
        return if candidate.is_absolute() {
            candidate
        } else {
            beads_dir.join(candidate)
        };
    }

    // Priority 4: File discovery (prefer issues.jsonl, fall back to beads.jsonl)
    if let Some(discovered) = discover_jsonl(beads_dir) {
        return discovered;
    }

    // Priority 5: Default (issues.jsonl) for writing when nothing exists
    beads_dir.join(DEFAULT_JSONL_FILENAME)
}

/// A configuration layer split into startup-only and runtime (DB) keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigLayer {
    pub startup: HashMap<String, String>,
    pub runtime: HashMap<String, String>,
}

impl ConfigLayer {
    /// Merge another layer on top of this one (higher precedence wins).
    ///
    /// Keys are normalized (hyphens replaced with underscores) before insertion
    /// so that `issue-prefix` (from YAML) and `issue_prefix` (from defaults)
    /// are treated as the same key and higher-precedence layers always win.
    pub fn merge_from(&mut self, other: &Self) {
        for (key, value) in &other.startup {
            let canonical = key.replace('-', "_");
            // Remove any variant of this key that already exists under a
            // different spelling (e.g. hyphenated vs underscored).
            if canonical == *key {
                let hyphenated = key.replace('_', "-");
                if hyphenated != *key {
                    self.startup.remove(&hyphenated);
                }
            } else {
                self.startup.remove(&canonical);
            }
            self.startup.insert(canonical, value.clone());
        }
        for (key, value) in &other.runtime {
            let canonical = key.replace('-', "_");
            if canonical == *key {
                let hyphenated = key.replace('_', "-");
                if hyphenated != *key {
                    self.runtime.remove(&hyphenated);
                }
            } else {
                self.runtime.remove(&canonical);
            }
            self.runtime.insert(canonical, value.clone());
        }
    }

    /// Merge multiple layers in precedence order (lowest to highest).
    #[must_use]
    pub fn merge_layers(layers: &[Self]) -> Self {
        let mut merged = Self::default();
        for layer in layers {
            merged.merge_from(layer);
        }
        merged
    }

    /// Build a layer from a YAML file path. Missing files return empty config.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read or parsed.
    pub fn from_yaml(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let contents = fs::read_to_string(path)?;
        let value: serde_yml::Value = serde_yml::from_str(&contents)?;
        Ok(layer_from_yaml_value(&value))
    }

    /// Build a layer from environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let mut layer = Self::default();

        for (key, value) in env::vars() {
            if let Some(stripped) = key.strip_prefix("BD_") {
                let normalized = stripped.to_lowercase();
                for variant in env_key_variants(&normalized) {
                    insert_key_value(&mut layer, &variant, value.clone());
                }
            }
        }

        if let Ok(value) = env::var("BEADS_FLUSH_DEBOUNCE") {
            insert_key_value(&mut layer, "flush-debounce", value);
        }
        if let Ok(value) = env::var("BEADS_IDENTITY") {
            insert_key_value(&mut layer, "identity", value);
        }
        if let Ok(value) = env::var("BEADS_REMOTE_SYNC_INTERVAL") {
            insert_key_value(&mut layer, "remote-sync-interval", value);
        }
        if let Ok(value) = env::var("BEADS_AUTO_START_DAEMON")
            && let Some(enabled) = parse_bool(&value)
        {
            insert_key_value(&mut layer, "no-daemon", (!enabled).to_string());
        }

        layer
    }

    /// Build a layer from DB config table values.
    ///
    /// # Errors
    ///
    /// Returns an error if config table lookup fails.
    pub fn from_db(storage: &SqliteStorage) -> Result<Self> {
        let mut layer = Self::default();
        let map = storage.get_all_config()?;
        for (key, value) in map {
            if is_startup_key(&key) {
                continue;
            }
            layer.runtime.insert(key, value);
        }
        Ok(layer)
    }
}

/// CLI overrides for config loading (optional).
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub db: Option<PathBuf>,
    pub actor: Option<String>,
    pub identity: Option<String>,
    pub json: Option<bool>,
    pub display_color: Option<bool>,
    pub quiet: Option<bool>,
    pub no_db: Option<bool>,
    pub no_daemon: Option<bool>,
    pub no_auto_flush: Option<bool>,
    pub no_auto_import: Option<bool>,
    pub lock_timeout: Option<u64>,
}

impl CliOverrides {
    #[must_use]
    pub fn as_layer(&self) -> ConfigLayer {
        let mut layer = ConfigLayer::default();

        if let Some(path) = &self.db {
            insert_key_value(&mut layer, "db", path.to_string_lossy().to_string());
        }
        if let Some(actor) = &self.actor {
            insert_key_value(&mut layer, "actor", actor.clone());
        }
        if let Some(identity) = &self.identity {
            insert_key_value(&mut layer, "identity", identity.clone());
        }
        if let Some(json) = self.json {
            insert_key_value(&mut layer, "json", json.to_string());
        }
        if let Some(display_color) = self.display_color {
            insert_key_value(&mut layer, "display.color", display_color.to_string());
        }
        if let Some(no_db) = self.no_db {
            insert_key_value(&mut layer, "no-db", no_db.to_string());
        }
        if let Some(no_daemon) = self.no_daemon {
            insert_key_value(&mut layer, "no-daemon", no_daemon.to_string());
        }
        if let Some(no_auto_flush) = self.no_auto_flush {
            insert_key_value(&mut layer, "no-auto-flush", no_auto_flush.to_string());
        }
        if let Some(no_auto_import) = self.no_auto_import {
            insert_key_value(&mut layer, "no-auto-import", no_auto_import.to_string());
        }
        if let Some(lock_timeout) = self.lock_timeout {
            insert_key_value(&mut layer, "lock-timeout", lock_timeout.to_string());
        }

        layer
    }
}

/// Load project config (.beads/config.yaml).
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_project_config(beads_dir: &Path) -> Result<ConfigLayer> {
    ConfigLayer::from_yaml(&beads_dir.join("config.yaml"))
}

/// Load user config (~/.config/beads/config.yaml), falling back to ~/.config/bd/config.yaml.
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_user_config() -> Result<ConfigLayer> {
    let Ok(home) = env::var("HOME") else {
        return Ok(ConfigLayer::default());
    };
    let config_root = Path::new(&home).join(".config");
    let beads_path = config_root.join("beads").join("config.yaml");
    if beads_path.exists() {
        return ConfigLayer::from_yaml(&beads_path);
    }
    let legacy_path = config_root.join("bd").join("config.yaml");
    ConfigLayer::from_yaml(&legacy_path)
}

/// Load legacy user config (~/.beads/config.yaml).
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read or parsed.
pub fn load_legacy_user_config() -> Result<ConfigLayer> {
    let Ok(home) = env::var("HOME") else {
        return Ok(ConfigLayer::default());
    };
    let path = Path::new(&home).join(".beads").join("config.yaml");
    ConfigLayer::from_yaml(&path)
}

/// Load startup-only configuration layers (YAML + env, no DB).
///
/// # Errors
///
/// Returns an error if any config file cannot be read or parsed.
pub fn load_startup_config(beads_dir: &Path) -> Result<ConfigLayer> {
    let legacy_user = load_legacy_user_config()?;
    let user = load_user_config()?;
    let project = load_project_config(beads_dir)?;
    let env_layer = ConfigLayer::from_env();

    Ok(ConfigLayer::merge_layers(&[
        legacy_user,
        user,
        project,
        env_layer,
    ]))
}

/// Default config layer (lowest precedence).
#[must_use]
pub fn default_config_layer() -> ConfigLayer {
    let mut layer = ConfigLayer::default();
    layer
        .runtime
        .insert("issue_prefix".to_string(), "bd".to_string());
    layer
}

/// Load configuration with classic precedence order.
///
/// # Errors
///
/// Returns an error if any config file cannot be read or parsed, or DB access fails.
pub fn load_config(
    beads_dir: &Path,
    storage: Option<&SqliteStorage>,
    cli: &CliOverrides,
) -> Result<ConfigLayer> {
    let defaults = default_config_layer();

    // Infer issue prefix from the first issue in JSONL so workspaces with
    // non-"bd" prefixes don't silently fall back to "bd" when the DB layer
    // is missing the stored prefix (e.g. after auto-rebuild).
    // Uses a fast single-line read (not full-file scan) since this runs on
    // every command.
    let jsonl_path = resolve_paths(beads_dir, cli.db.as_ref())?.jsonl_path;
    let mut jsonl_inferred = ConfigLayer::default();
    if let Some(prefix) = first_prefix_from_jsonl(&jsonl_path)? {
        jsonl_inferred
            .runtime
            .insert("issue_prefix".to_string(), prefix);
    }

    let db_layer = match storage {
        Some(storage) => ConfigLayer::from_db(storage)?,
        None => ConfigLayer::default(),
    };
    let legacy_user = load_legacy_user_config()?;
    let user = load_user_config()?;
    let project = load_project_config(beads_dir)?;
    let env_layer = ConfigLayer::from_env();
    let cli_layer = cli.as_layer();

    Ok(ConfigLayer::merge_layers(&[
        defaults,
        jsonl_inferred,
        db_layer,
        legacy_user,
        user,
        project,
        env_layer,
        cli_layer,
    ]))
}

/// Build ID generation config from a merged config layer.
#[must_use]
pub fn id_config_from_layer(layer: &ConfigLayer) -> IdConfig {
    let prefix = get_value(layer, &["issue_prefix", "issue-prefix", "prefix"])
        .cloned()
        .filter(|p| !p.trim().is_empty())
        .unwrap_or_else(|| "bd".to_string());

    let min_hash_length = parse_usize(layer, &["min_hash_length", "min-hash-length"]).unwrap_or(3);
    let max_hash_length = parse_usize(layer, &["max_hash_length", "max-hash-length"]).unwrap_or(8);
    let max_collision_prob =
        parse_f64(layer, &["max_collision_prob", "max-collision-prob"]).unwrap_or(0.25);

    IdConfig {
        prefix,
        min_hash_length,
        max_hash_length,
        max_collision_prob,
    }
}

/// Resolve default priority for new issues from config.
///
/// # Errors
///
/// Returns an error if the configured value is not a valid priority (0-4).
pub fn default_priority_from_layer(layer: &ConfigLayer) -> Result<Priority> {
    get_value(layer, &["default_priority", "default-priority"])
        .map_or_else(|| Ok(Priority::MEDIUM), |value| Priority::from_str(value))
}

/// Resolve default issue type for new issues from config.
///
/// # Errors
///
/// Returns an error only if parsing fails (custom types are allowed).
pub fn default_issue_type_from_layer(layer: &ConfigLayer) -> Result<IssueType> {
    get_value(layer, &["default_type", "default-type"])
        .map_or_else(|| Ok(IssueType::Task), |value| IssueType::from_str(value))
}

/// Resolve display color preference from a merged config layer.
///
/// Accepts keys: `display.color`, `display-color`, `display_color`.
#[must_use]
pub fn display_color_from_layer(layer: &ConfigLayer) -> Option<bool> {
    get_value(layer, &["display.color", "display-color", "display_color"])
        .and_then(|value| parse_bool(value))
}

/// Determine whether human-readable output should use ANSI color.
///
/// Precedence:
/// 1) Config `display.color` (if set)
/// 2) `NO_COLOR` environment variable (standard)
/// 3) stdout is a terminal
#[must_use]
pub fn should_use_color(layer: &ConfigLayer) -> bool {
    if let Some(value) = display_color_from_layer(layer) {
        return value;
    }
    if env::var_os("NO_COLOR").is_some() {
        return false;
    }
    std::io::stdout().is_terminal()
}

/// Resolve external project mappings from config.
///
/// Supports `external_projects.<name>` or `external-projects.<name>` keys.
/// Relative paths are resolved against the project root (parent of `.beads`).
#[must_use]
pub fn external_projects_from_layer(
    layer: &ConfigLayer,
    beads_dir: &Path,
) -> HashMap<String, PathBuf> {
    let base_dir = beads_dir.parent().unwrap_or(beads_dir);
    let mut map = HashMap::new();
    // Startup keys are lower precedence than runtime keys in merged config.
    // Insert startup first so runtime values win on duplicate project names.
    let iter = layer.startup.iter().chain(layer.runtime.iter());

    for (key, value) in iter {
        let key_lower = key.to_lowercase();
        let is_external = key_lower.starts_with("external_projects.")
            || key_lower.starts_with("external-projects.");
        if !is_external {
            continue;
        }

        let project = key.split_once('.').map(|(_, rest)| rest);
        let Some(project) = project.filter(|p| !p.trim().is_empty()) else {
            continue;
        };

        let path = PathBuf::from(value.trim());
        let resolved = if path.is_absolute() {
            path
        } else {
            base_dir.join(path)
        };
        map.insert(project.trim().to_string(), resolved);
    }

    map
}

/// Resolve external project DB paths from config.
///
/// Projects are expected to be either a `.beads` directory or a project root
/// containing `.beads/`.
#[must_use]
pub fn external_project_db_paths(
    layer: &ConfigLayer,
    beads_dir: &Path,
) -> HashMap<String, PathBuf> {
    let projects = external_projects_from_layer(layer, beads_dir);
    let mut db_paths = HashMap::new();

    for (name, path) in projects {
        let beads_path = if path.file_name().is_some_and(|name| name == ".beads") {
            path.clone()
        } else {
            path.join(".beads")
        };

        if !beads_path.is_dir() {
            warn!(
                project = %name,
                path = %beads_path.display(),
                "External project .beads directory not found"
            );
            continue;
        }

        match ConfigPaths::resolve(&beads_path, None) {
            Ok(paths) => {
                db_paths.insert(name, paths.db_path);
            }
            Err(err) => {
                warn!(
                    project = %name,
                    path = %beads_path.display(),
                    error = %err,
                    "Failed to resolve external project DB path"
                );
            }
        }
    }

    db_paths
}

/// Resolve actor from a merged config layer.
#[must_use]
pub fn actor_from_layer(layer: &ConfigLayer) -> Option<String> {
    get_startup_value(layer, &["actor"])
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Resolve actor with fallback to USER and a safe default.
#[must_use]
pub fn resolve_actor(layer: &ConfigLayer) -> String {
    actor_from_layer(layer)
        .or_else(|| {
            std::env::var("USER")
                .ok()
                .map(|value| value.trim().to_string())
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Read the `claim-exclusive` config key.
///
/// When true, `--claim` rejects re-claims even by the same actor.
/// Accepts `claim.exclusive`, `claim_exclusive`, or `claim-exclusive`.
#[must_use]
pub fn claim_exclusive_from_layer(layer: &ConfigLayer) -> bool {
    get_startup_value(layer, &["claim-exclusive", "claim.exclusive"])
        .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1")
}

/// Determine if a key is startup-only.
///
/// Startup-only keys can only be set in YAML config files, not in the database.
/// These include path settings, behavior flags, and git-related options.
#[must_use]
pub fn is_startup_key(key: &str) -> bool {
    let normalized = normalize_key(key);

    if normalized.starts_with("git.")
        || normalized.starts_with("routing.")
        || normalized.starts_with("validation.")
        || normalized.starts_with("directory.")
        || normalized.starts_with("sync.")
        || normalized.starts_with("external-projects.")
    {
        return true;
    }

    matches!(
        normalized.as_str(),
        "no-db"
            | "no-daemon"
            | "no-auto-flush"
            | "no-auto-import"
            | "json"
            | "db"
            | "actor"
            | "identity"
            | "flush-debounce"
            | "lock-timeout"
            | "remote-sync-interval"
            | "no-git-ops"
            | "no-push"
            | "sync-branch"
            | "sync.branch"
            | "external-projects"
            | "hierarchy.max-depth"
    )
}

fn insert_key_value(layer: &mut ConfigLayer, key: &str, value: String) {
    // Normalize hyphens to underscores so YAML keys like `issue-prefix`
    // are stored under the same canonical key as `issue_prefix`.
    let canonical = key.replace('-', "_");
    if is_startup_key(key) {
        layer.startup.insert(canonical, value);
    } else {
        layer.runtime.insert(canonical, value);
    }
}

fn normalize_key(key: &str) -> String {
    key.trim().to_lowercase().replace('_', "-")
}

fn env_key_variants(raw: &str) -> Vec<String> {
    let mut variants = Vec::new();
    let raw_lower = raw.to_lowercase();
    variants.push(raw_lower.clone());
    variants.push(raw_lower.replace('_', "."));
    variants.push(raw_lower.replace('_', "-"));
    variants
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn get_startup_value<'a>(layer: &'a ConfigLayer, keys: &[&str]) -> Option<&'a String> {
    let normalized_keys: Vec<String> = keys.iter().map(|key| normalize_key(key)).collect();
    for (key, value) in &layer.startup {
        let normalized = normalize_key(key);
        if normalized_keys
            .iter()
            .any(|candidate| candidate == &normalized)
        {
            return Some(value);
        }
    }
    None
}

fn get_value<'a>(layer: &'a ConfigLayer, keys: &[&str]) -> Option<&'a String> {
    let normalized_keys: Vec<String> = keys.iter().map(|key| normalize_key(key)).collect();
    for (key, value) in &layer.runtime {
        let normalized = normalize_key(key);
        if normalized_keys
            .iter()
            .any(|candidate| candidate == &normalized)
        {
            return Some(value);
        }
    }
    None
}

fn parse_usize(layer: &ConfigLayer, keys: &[&str]) -> Option<usize> {
    get_value(layer, keys).and_then(|value| value.trim().parse::<usize>().ok())
}

fn parse_f64(layer: &ConfigLayer, keys: &[&str]) -> Option<f64> {
    get_value(layer, keys).and_then(|value| value.trim().parse::<f64>().ok())
}

fn db_override_from_layer(layer: &ConfigLayer) -> Option<PathBuf> {
    get_startup_value(layer, &["db", "database"]).and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(PathBuf::from(trimmed))
        }
    })
}

fn lock_timeout_from_layer(layer: &ConfigLayer) -> Option<u64> {
    get_startup_value(layer, &["lock-timeout", "lock_timeout"])
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn layer_from_yaml_value(value: &serde_yml::Value) -> ConfigLayer {
    let mut layer = ConfigLayer::default();
    let mut flat = HashMap::new();
    flatten_yaml(value, "", &mut flat);

    for (key, value) in flat {
        insert_key_value(&mut layer, &key, value);
    }

    layer
}

fn flatten_yaml(value: &serde_yml::Value, prefix: &str, out: &mut HashMap<String, String>) {
    match value {
        serde_yml::Value::Mapping(map) => {
            for (key, value) in map {
                let Some(key_str) = key.as_str() else {
                    continue;
                };
                let next_prefix = if prefix.is_empty() {
                    key_str.to_string()
                } else {
                    format!("{prefix}.{key_str}")
                };
                flatten_yaml(value, &next_prefix, out);
            }
        }
        serde_yml::Value::Sequence(values) => {
            let joined = values
                .iter()
                .filter_map(yaml_scalar_to_string)
                .collect::<Vec<_>>()
                .join(",");
            out.insert(prefix.to_string(), joined);
        }
        _ => {
            if let Some(value) = yaml_scalar_to_string(value) {
                out.insert(prefix.to_string(), value);
            }
        }
    }
}

fn yaml_scalar_to_string(value: &serde_yml::Value) -> Option<String> {
    match value {
        serde_yml::Value::Bool(v) => Some(v.to_string()),
        serde_yml::Value::Number(n) => Some(n.to_string()),
        serde_yml::Value::String(s) => Some(s.clone()),
        serde_yml::Value::Null | serde_yml::Value::Sequence(_) | serde_yml::Value::Mapping(_) => {
            None
        }
        serde_yml::Value::Tagged(tagged) => yaml_scalar_to_string(&tagged.value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Issue, IssueType, Priority};
    use crate::storage::SqliteStorage;
    use chrono::Utc;
    use tempfile::TempDir;

    fn write_single_issue_jsonl(path: &Path, id: &str, title: &str) {
        let now = Utc::now();
        let issue = Issue {
            id: id.to_string(),
            title: title.to_string(),
            created_at: now,
            updated_at: now,
            ..Issue::default()
        };
        let json = serde_json::to_string(&issue).expect("serialize issue");
        fs::write(path, format!("{json}\n")).expect("write jsonl");
    }

    #[test]
    fn metadata_defaults_when_missing() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata = Metadata::load(&beads_dir).expect("metadata");
        assert_eq!(metadata.database, DEFAULT_DB_FILENAME);
        assert_eq!(metadata.jsonl_export, DEFAULT_JSONL_FILENAME);
    }

    #[test]
    fn metadata_override_paths() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata_path = beads_dir.join("metadata.json");
        let metadata = r#"{"database": "custom.db", "jsonl_export": "custom.jsonl"}"#;
        fs::write(metadata_path, metadata).expect("write metadata");

        let paths = ConfigPaths::resolve(&beads_dir, None).expect("paths");
        assert_eq!(paths.db_path, beads_dir.join("custom.db"));
        assert_eq!(paths.jsonl_path, beads_dir.join("custom.jsonl"));
    }

    #[test]
    fn merge_precedence_order() {
        let mut defaults = default_config_layer();
        defaults
            .runtime
            .insert("issue_prefix".to_string(), "bd".to_string());

        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "db".to_string());

        let mut yaml = ConfigLayer::default();
        yaml.runtime
            .insert("issue_prefix".to_string(), "yaml".to_string());

        let mut env_layer = ConfigLayer::default();
        env_layer
            .runtime
            .insert("issue_prefix".to_string(), "env".to_string());

        let mut cli = ConfigLayer::default();
        cli.runtime
            .insert("issue_prefix".to_string(), "cli".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db, yaml, env_layer, cli]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "cli");
    }

    #[test]
    fn yaml_startup_keys_are_separated() {
        let yaml = r"
no-db: true
issue_prefix: bd
";
        let value: serde_yml::Value = serde_yml::from_str(yaml).expect("parse yaml");
        let layer = layer_from_yaml_value(&value);
        assert_eq!(layer.startup.get("no_db").unwrap(), "true");
        assert_eq!(layer.runtime.get("issue_prefix").unwrap(), "bd");
    }

    #[test]
    fn yaml_sequence_flattens_to_csv() {
        let yaml = r"
labels:
  - backend
  - api
";
        let value: serde_yml::Value = serde_yml::from_str(yaml).expect("parse yaml");
        let layer = layer_from_yaml_value(&value);
        assert_eq!(layer.runtime.get("labels").unwrap(), "backend,api");
    }

    #[test]
    fn id_config_parses_numeric_overrides() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("issue_prefix".to_string(), "br".to_string());
        layer
            .runtime
            .insert("min_hash_length".to_string(), "4".to_string());
        layer
            .runtime
            .insert("max_hash_length".to_string(), "10".to_string());
        layer
            .runtime
            .insert("max_collision_prob".to_string(), "0.5".to_string());

        let config = id_config_from_layer(&layer);
        assert_eq!(config.prefix, "br");
        assert_eq!(config.min_hash_length, 4);
        assert_eq!(config.max_hash_length, 10);
        assert!((config.max_collision_prob - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn default_priority_from_layer_uses_config_value() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("default_priority".to_string(), "1".to_string());

        let priority = default_priority_from_layer(&layer).expect("default priority");
        assert_eq!(priority, Priority::HIGH);
    }

    #[test]
    fn default_priority_from_layer_errors_on_invalid_value() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("default_priority".to_string(), "9".to_string());

        assert!(default_priority_from_layer(&layer).is_err());
    }

    #[test]
    fn default_issue_type_from_layer_uses_config_value() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("default_type".to_string(), "feature".to_string());

        let issue_type = default_issue_type_from_layer(&layer).expect("default type");
        assert_eq!(issue_type, IssueType::Feature);
    }

    #[test]
    fn db_layer_skips_startup_keys() {
        let mut storage = SqliteStorage::open_memory().expect("storage");
        storage.set_config("no-db", "true").expect("set no-db");
        storage
            .set_config("issue_prefix", "bd")
            .expect("set issue_prefix");

        let layer = ConfigLayer::from_db(&storage).expect("db layer");
        assert!(!layer.startup.contains_key("no-db"));
        assert_eq!(layer.runtime.get("issue_prefix").unwrap(), "bd");
    }

    #[test]
    fn startup_layer_reads_db_override() {
        let mut layer = ConfigLayer::default();
        layer
            .startup
            .insert("db".to_string(), "/tmp/beads.db".to_string());

        let override_path = db_override_from_layer(&layer).expect("db override");
        assert_eq!(override_path, PathBuf::from("/tmp/beads.db"));
    }

    #[test]
    fn startup_layer_reads_lock_timeout() {
        let mut layer = ConfigLayer::default();
        layer
            .startup
            .insert("lock_timeout".to_string(), "2500".to_string());

        let timeout = lock_timeout_from_layer(&layer).expect("lock timeout");
        assert_eq!(timeout, 2500);
    }

    // ==================== Additional Config Unit Tests ====================
    // Tests for beads_rust-7h9: Config unit tests - Layered configuration

    #[test]
    fn precedence_default_is_lowest() {
        // Verify that default layer values are overridden by any other layer
        let defaults = default_config_layer();
        assert_eq!(defaults.runtime.get("issue_prefix").unwrap(), "bd");

        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "from_db".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "from_db");
    }

    #[test]
    fn precedence_db_overrides_default() {
        let defaults = default_config_layer();
        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "db_prefix".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "db_prefix");
    }

    #[test]
    fn precedence_yaml_overrides_db() {
        let defaults = default_config_layer();
        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "db_prefix".to_string());
        let mut yaml = ConfigLayer::default();
        yaml.runtime
            .insert("issue_prefix".to_string(), "yaml_prefix".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db, yaml]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "yaml_prefix");
    }

    #[test]
    fn precedence_env_overrides_yaml() {
        let defaults = default_config_layer();
        let mut yaml = ConfigLayer::default();
        yaml.runtime
            .insert("issue_prefix".to_string(), "yaml_prefix".to_string());
        let mut env_layer = ConfigLayer::default();
        env_layer
            .runtime
            .insert("issue_prefix".to_string(), "env_prefix".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, yaml, env_layer]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "env_prefix");
    }

    #[test]
    fn precedence_cli_overrides_all() {
        let defaults = default_config_layer();
        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "db".to_string());
        let mut yaml = ConfigLayer::default();
        yaml.runtime
            .insert("issue_prefix".to_string(), "yaml".to_string());
        let mut env_layer = ConfigLayer::default();
        env_layer
            .runtime
            .insert("issue_prefix".to_string(), "env".to_string());
        let mut cli = ConfigLayer::default();
        cli.runtime
            .insert("issue_prefix".to_string(), "cli_wins".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db, yaml, env_layer, cli]);
        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "cli_wins");
    }

    #[test]
    fn precedence_chain_includes_legacy_and_user_layers() {
        let defaults = default_config_layer();

        let mut db = ConfigLayer::default();
        db.runtime
            .insert("issue_prefix".to_string(), "db".to_string());

        let mut legacy = ConfigLayer::default();
        legacy
            .runtime
            .insert("issue_prefix".to_string(), "legacy".to_string());

        let mut user = ConfigLayer::default();
        user.runtime
            .insert("issue_prefix".to_string(), "user".to_string());

        let mut project = ConfigLayer::default();
        project
            .runtime
            .insert("issue_prefix".to_string(), "project".to_string());

        let mut env_layer = ConfigLayer::default();
        env_layer
            .runtime
            .insert("issue_prefix".to_string(), "env".to_string());

        let mut cli = ConfigLayer::default();
        cli.runtime
            .insert("issue_prefix".to_string(), "cli".to_string());

        let merged =
            ConfigLayer::merge_layers(&[defaults, db, legacy, user, project, env_layer, cli]);

        assert_eq!(merged.runtime.get("issue_prefix").unwrap(), "cli");
    }

    #[test]
    fn precedence_full_chain_with_different_keys() {
        // Each layer sets a different key, all should be preserved
        let mut defaults = default_config_layer();
        defaults
            .runtime
            .insert("from_default".to_string(), "default_value".to_string());

        let mut db = ConfigLayer::default();
        db.runtime
            .insert("from_db".to_string(), "db_value".to_string());

        let mut yaml = ConfigLayer::default();
        yaml.runtime
            .insert("from_yaml".to_string(), "yaml_value".to_string());

        let mut env_layer = ConfigLayer::default();
        env_layer
            .runtime
            .insert("from_env".to_string(), "env_value".to_string());

        let mut cli = ConfigLayer::default();
        cli.runtime
            .insert("from_cli".to_string(), "cli_value".to_string());

        let merged = ConfigLayer::merge_layers(&[defaults, db, yaml, env_layer, cli]);

        assert_eq!(merged.runtime.get("from_default").unwrap(), "default_value");
        assert_eq!(merged.runtime.get("from_db").unwrap(), "db_value");
        assert_eq!(merged.runtime.get("from_yaml").unwrap(), "yaml_value");
        assert_eq!(merged.runtime.get("from_env").unwrap(), "env_value");
        assert_eq!(merged.runtime.get("from_cli").unwrap(), "cli_value");
    }

    #[test]
    fn metadata_handles_empty_strings() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Write metadata with empty strings
        let metadata_path = beads_dir.join("metadata.json");
        let metadata = r#"{"database": "", "jsonl_export": "  "}"#;
        fs::write(metadata_path, metadata).expect("write metadata");

        let loaded = Metadata::load(&beads_dir).expect("metadata");
        // Empty strings should fall back to defaults
        assert_eq!(loaded.database, DEFAULT_DB_FILENAME);
        assert_eq!(loaded.jsonl_export, DEFAULT_JSONL_FILENAME);
    }

    #[test]
    fn metadata_handles_extra_fields() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Write metadata with extra fields (should be ignored)
        let metadata_path = beads_dir.join("metadata.json");
        let metadata =
            r#"{"database": "test.db", "jsonl_export": "test.jsonl", "unknown_field": true}"#;
        fs::write(metadata_path, metadata).expect("write metadata");

        let loaded = Metadata::load(&beads_dir).expect("metadata");
        assert_eq!(loaded.database, "test.db");
        assert_eq!(loaded.jsonl_export, "test.jsonl");
    }

    #[test]
    fn metadata_with_backend_and_retention() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata_path = beads_dir.join("metadata.json");
        let metadata = r#"{"database": "beads.db", "jsonl_export": "issues.jsonl", "backend": "sqlite", "deletions_retention_days": 30}"#;
        fs::write(metadata_path, metadata).expect("write metadata");

        let loaded = Metadata::load(&beads_dir).expect("metadata");
        assert_eq!(loaded.backend, Some("sqlite".to_string()));
        assert_eq!(loaded.deletions_retention_days, Some(30));
    }

    #[test]
    fn discover_beads_dir_returns_error_when_not_found() {
        let temp = TempDir::new().expect("tempdir");
        // No .beads directory created

        let result = discover_beads_dir(Some(temp.path()));
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BeadsError::NotInitialized));
    }

    #[test]
    fn discover_beads_dir_finds_at_root() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let discovered = discover_beads_dir(Some(temp.path())).expect("discover");
        assert_eq!(discovered, beads_dir);
    }

    #[test]
    fn discover_beads_dir_deeply_nested() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Create deeply nested directory
        let nested = temp
            .path()
            .join("a")
            .join("b")
            .join("c")
            .join("d")
            .join("e");
        fs::create_dir_all(&nested).expect("create nested");

        let discovered = discover_beads_dir(Some(&nested)).expect("discover");
        assert_eq!(discovered, beads_dir);
    }

    #[test]
    fn discover_optional_beads_dir_with_cli_uses_explicit_db_override() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join("external").join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let cli = CliOverrides {
            db: Some(beads_dir.join("beads.db")),
            ..CliOverrides::default()
        };

        let discovered =
            discover_optional_beads_dir_with_cli(&cli).expect("optional discovery with db");
        assert_eq!(discovered, Some(beads_dir));
    }

    #[test]
    fn discover_optional_beads_dir_with_cli_surfaces_invalid_db_override() {
        let cli = CliOverrides {
            db: Some(PathBuf::from("/tmp/not-a-beads-db")),
            ..CliOverrides::default()
        };

        let err = discover_optional_beads_dir_with_cli(&cli)
            .expect_err("invalid explicit db should error");
        assert!(matches!(err, BeadsError::Validation { .. }));
    }

    #[test]
    fn env_key_variants_generates_all_forms() {
        let variants = env_key_variants("no_auto_flush");
        assert!(variants.contains(&"no_auto_flush".to_string()));
        assert!(variants.contains(&"no.auto.flush".to_string()));
        assert!(variants.contains(&"no-auto-flush".to_string()));
    }

    #[test]
    fn normalize_key_handles_various_formats() {
        assert_eq!(normalize_key("ISSUE_PREFIX"), "issue-prefix");
        assert_eq!(normalize_key("issue-prefix"), "issue-prefix");
        assert_eq!(normalize_key("issue_prefix"), "issue-prefix");
        assert_eq!(normalize_key("  ISSUE_PREFIX  "), "issue-prefix");
    }

    #[test]
    fn parse_bool_handles_all_truthy_values() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("yes"), Some(true));
        assert_eq!(parse_bool("YES"), Some(true));
        assert_eq!(parse_bool("y"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));
    }

    #[test]
    fn parse_bool_handles_all_falsy_values() {
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("no"), Some(false));
        assert_eq!(parse_bool("NO"), Some(false));
        assert_eq!(parse_bool("n"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
    }

    #[test]
    fn parse_bool_returns_none_for_invalid() {
        assert_eq!(parse_bool("maybe"), None);
        assert_eq!(parse_bool(""), None);
        assert_eq!(parse_bool("2"), None);
    }

    #[test]
    fn is_startup_key_identifies_startup_keys() {
        assert!(is_startup_key("no-db"));
        assert!(is_startup_key("no-daemon"));
        assert!(is_startup_key("no-auto-flush"));
        assert!(is_startup_key("no-auto-import"));
        assert!(is_startup_key("json"));
        assert!(is_startup_key("db"));
        assert!(is_startup_key("actor"));
        assert!(is_startup_key("identity"));
        assert!(is_startup_key("lock-timeout"));
        assert!(is_startup_key("git.branch")); // prefix check
        assert!(is_startup_key("routing.policy")); // prefix check
    }

    #[test]
    fn is_startup_key_identifies_runtime_keys() {
        assert!(!is_startup_key("issue_prefix"));
        assert!(!is_startup_key("issue-prefix"));
        assert!(!is_startup_key("min_hash_length"));
        assert!(!is_startup_key("labels"));
    }

    #[test]
    fn resolve_db_path_absolute_in_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let absolute_path = "/absolute/path/to/beads.db";
        let metadata = Metadata {
            database: absolute_path.to_string(),
            jsonl_export: DEFAULT_JSONL_FILENAME.to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_db_path(&beads_dir, &metadata, None);
        assert_eq!(resolved, PathBuf::from(absolute_path));
    }

    #[test]
    fn resolve_db_path_relative_in_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata = Metadata {
            database: "relative.db".to_string(),
            jsonl_export: DEFAULT_JSONL_FILENAME.to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_db_path(&beads_dir, &metadata, None);
        assert_eq!(resolved, beads_dir.join("relative.db"));
    }

    #[test]
    fn resolve_db_path_override_wins() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata = Metadata::default();
        let override_path = PathBuf::from("/override/path.db");

        let resolved = resolve_db_path(&beads_dir, &metadata, Some(&override_path));
        assert_eq!(resolved, override_path);
    }

    #[test]
    fn resolve_jsonl_path_absolute_in_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let absolute_path = "/absolute/path/to/issues.jsonl";
        let metadata = Metadata {
            database: DEFAULT_DB_FILENAME.to_string(),
            jsonl_export: absolute_path.to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);
        assert_eq!(resolved, PathBuf::from(absolute_path));
    }

    #[test]
    fn resolve_jsonl_path_relative_in_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata = Metadata {
            database: DEFAULT_DB_FILENAME.to_string(),
            jsonl_export: "relative.jsonl".to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);
        assert_eq!(resolved, beads_dir.join("relative.jsonl"));
    }

    #[test]
    fn resolve_jsonl_path_db_override_derives_sibling() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let metadata = Metadata::default();
        let db_override = PathBuf::from("/some/path/custom.db");

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, Some(&db_override));
        assert_eq!(resolved, PathBuf::from("/some/path/issues.jsonl"));
    }

    #[test]
    fn cli_overrides_as_layer_sets_startup_keys() {
        let cli = CliOverrides {
            db: Some(PathBuf::from("/cli/path.db")),
            actor: Some("cli_actor".to_string()),
            json: Some(true),
            display_color: None,
            quiet: None,
            no_db: Some(true),
            no_daemon: Some(true),
            no_auto_flush: Some(true),
            no_auto_import: Some(true),
            lock_timeout: Some(5000),
            identity: None,
        };

        let layer = cli.as_layer();

        assert_eq!(layer.startup.get("db").unwrap(), "/cli/path.db");
        assert_eq!(layer.startup.get("actor").unwrap(), "cli_actor");
        assert_eq!(layer.startup.get("json").unwrap(), "true");
        assert_eq!(layer.startup.get("no_db").unwrap(), "true");
        assert_eq!(layer.startup.get("no_daemon").unwrap(), "true");
        assert_eq!(layer.startup.get("no_auto_flush").unwrap(), "true");
        assert_eq!(layer.startup.get("no_auto_import").unwrap(), "true");
        assert_eq!(layer.startup.get("lock_timeout").unwrap(), "5000");
    }

    #[test]
    fn cli_overrides_empty_produces_empty_layer() {
        let cli = CliOverrides::default();
        let layer = cli.as_layer();

        assert!(layer.startup.is_empty());
        assert!(layer.runtime.is_empty());
    }

    #[test]
    fn yaml_nested_keys_flatten_with_dots() {
        let yaml = r"
sync:
  branch: main
git:
  auto_commit: true
routing:
  policy: fifo
";
        let value: serde_yml::Value = serde_yml::from_str(yaml).expect("parse yaml");
        let layer = layer_from_yaml_value(&value);

        // git.* and routing.* prefixes go to startup (per is_startup_key)
        // sync.branch is an explicit startup key
        assert!(layer.startup.contains_key("sync.branch"));
        assert!(layer.startup.contains_key("git.auto_commit"));
        assert!(layer.startup.contains_key("routing.policy"));
    }

    #[test]
    fn actor_from_layer_returns_none_for_empty() {
        let layer = ConfigLayer::default();
        assert!(actor_from_layer(&layer).is_none());

        let mut layer_with_empty = ConfigLayer::default();
        layer_with_empty
            .startup
            .insert("actor".to_string(), "   ".to_string());
        assert!(actor_from_layer(&layer_with_empty).is_none());
    }

    #[test]
    fn actor_from_layer_returns_trimmed_value() {
        let mut layer = ConfigLayer::default();
        layer
            .startup
            .insert("actor".to_string(), "  test_actor  ".to_string());

        let actor = actor_from_layer(&layer).expect("actor");
        assert_eq!(actor, "test_actor");
    }

    #[test]
    fn external_projects_runtime_mapping_overrides_startup_mapping() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let mut layer = ConfigLayer::default();
        layer.startup.insert(
            "external_projects.shared".to_string(),
            "startup-path".to_string(),
        );
        layer.runtime.insert(
            "external_projects.shared".to_string(),
            "runtime-path".to_string(),
        );

        let projects = external_projects_from_layer(&layer, &beads_dir);
        assert_eq!(
            projects.get("shared"),
            Some(&temp.path().join("runtime-path")),
            "Runtime config should override lower-precedence startup config"
        );
    }

    #[test]
    fn resolved_jsonl_path_drives_prefix_inference() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        fs::write(
            beads_dir.join("metadata.json"),
            r#"{"database":"beads.db","jsonl_export":"custom.jsonl"}"#,
        )
        .expect("write metadata");
        write_single_issue_jsonl(&beads_dir.join("custom.jsonl"), "br-abc123", "custom issue");

        let paths = resolve_paths(&beads_dir, None).expect("resolve paths");
        assert_eq!(
            paths.jsonl_path,
            beads_dir.join("custom.jsonl"),
            "Metadata override should determine the active JSONL path"
        );
        assert_eq!(
            first_prefix_from_jsonl(&paths.jsonl_path).expect("infer prefix"),
            Some("br".to_string()),
            "Prefix inference should read from the resolved JSONL path"
        );
    }

    #[test]
    fn resolve_actor_falls_back_to_unknown() {
        let layer = ConfigLayer::default();
        // This test assumes USER env var may not be set in test context
        // or we need to verify the fallback mechanism
        let actor = resolve_actor(&layer);
        // Should be either USER env value or "unknown"
        assert!(!actor.is_empty());
    }

    #[test]
    fn merge_from_overwrites_existing_keys() {
        let mut base = ConfigLayer::default();
        base.runtime
            .insert("key1".to_string(), "base_value".to_string());
        base.startup
            .insert("key2".to_string(), "base_startup".to_string());

        let mut override_layer = ConfigLayer::default();
        override_layer
            .runtime
            .insert("key1".to_string(), "override_value".to_string());
        override_layer
            .startup
            .insert("key2".to_string(), "override_startup".to_string());

        base.merge_from(&override_layer);

        assert_eq!(base.runtime.get("key1").unwrap(), "override_value");
        assert_eq!(base.startup.get("key2").unwrap(), "override_startup");
    }

    #[test]
    fn merge_from_preserves_non_conflicting_keys() {
        let mut base = ConfigLayer::default();
        base.runtime
            .insert("base_only".to_string(), "base_value".to_string());

        let mut override_layer = ConfigLayer::default();
        override_layer
            .runtime
            .insert("override_only".to_string(), "override_value".to_string());

        base.merge_from(&override_layer);

        assert_eq!(base.runtime.get("base_only").unwrap(), "base_value");
        assert_eq!(base.runtime.get("override_only").unwrap(), "override_value");
    }

    #[test]
    fn config_paths_resolve_with_default_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let paths = ConfigPaths::resolve(&beads_dir, None).expect("paths");

        assert_eq!(paths.beads_dir, beads_dir);
        assert_eq!(paths.db_path, beads_dir.join(DEFAULT_DB_FILENAME));
        assert_eq!(paths.jsonl_path, beads_dir.join(DEFAULT_JSONL_FILENAME));
        assert_eq!(paths.metadata, Metadata::default());
    }

    #[test]
    fn load_project_config_returns_empty_when_missing() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        let layer = load_project_config(&beads_dir).expect("project config");
        assert!(layer.startup.is_empty());
        assert!(layer.runtime.is_empty());
    }

    #[test]
    fn load_project_config_parses_yaml() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        fs::write(
            beads_dir.join("config.yaml"),
            "issue_prefix: proj\nno-db: false\n",
        )
        .expect("write config");

        let layer = load_project_config(&beads_dir).expect("project config");
        assert_eq!(layer.runtime.get("issue_prefix").unwrap(), "proj");
        assert_eq!(layer.startup.get("no_db").unwrap(), "false");
    }

    #[test]
    fn id_config_uses_defaults_when_keys_missing() {
        let layer = ConfigLayer::default();
        let config = id_config_from_layer(&layer);

        assert_eq!(config.prefix, "bd");
        assert_eq!(config.min_hash_length, 3);
        assert_eq!(config.max_hash_length, 8);
        assert!((config.max_collision_prob - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn id_config_handles_hyphenated_keys() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("issue-prefix".to_string(), "hyphen".to_string());
        layer
            .runtime
            .insert("min-hash-length".to_string(), "5".to_string());

        let config = id_config_from_layer(&layer);
        assert_eq!(config.prefix, "hyphen");
        assert_eq!(config.min_hash_length, 5);
    }

    #[test]
    fn id_config_accepts_legacy_prefix_key() {
        let mut layer = ConfigLayer::default();
        layer
            .runtime
            .insert("prefix".to_string(), "legacy".to_string());

        let config = id_config_from_layer(&layer);
        assert_eq!(config.prefix, "legacy");
    }

    // ==================== JSONL Discovery Tests ====================
    // Tests for beads_rust-ndl: JSONL discovery + metadata.json handling

    #[test]
    fn discover_jsonl_prefers_issues_over_legacy() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Create both files
        fs::write(beads_dir.join("issues.jsonl"), "{}").expect("write issues");
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");

        let discovered = discover_jsonl(&beads_dir).expect("should discover");
        assert_eq!(discovered, beads_dir.join("issues.jsonl"));
    }

    #[test]
    fn discover_jsonl_falls_back_to_legacy() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Only legacy file exists
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");

        let discovered = discover_jsonl(&beads_dir).expect("should discover");
        assert_eq!(discovered, beads_dir.join("beads.jsonl"));
    }

    #[test]
    fn discover_jsonl_returns_none_when_empty() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // No JSONL files
        let discovered = discover_jsonl(&beads_dir);
        assert!(discovered.is_none());
    }

    #[test]
    fn discover_jsonl_ignores_merge_artifacts() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Only merge artifacts exist (should not be discovered)
        fs::write(beads_dir.join("beads.base.jsonl"), "{}").expect("write base");
        fs::write(beads_dir.join("beads.left.jsonl"), "{}").expect("write left");
        fs::write(beads_dir.join("beads.right.jsonl"), "{}").expect("write right");

        let discovered = discover_jsonl(&beads_dir);
        assert!(discovered.is_none());
    }

    #[test]
    fn is_excluded_jsonl_detects_merge_artifacts() {
        assert!(is_excluded_jsonl("beads.base.jsonl"));
        assert!(is_excluded_jsonl("beads.left.jsonl"));
        assert!(is_excluded_jsonl("beads.right.jsonl"));
    }

    #[test]
    fn is_excluded_jsonl_detects_deletion_log() {
        assert!(is_excluded_jsonl("deletions.jsonl"));
    }

    #[test]
    fn is_excluded_jsonl_detects_interaction_log() {
        assert!(is_excluded_jsonl("interactions.jsonl"));
    }

    #[test]
    fn is_excluded_jsonl_allows_valid_files() {
        assert!(!is_excluded_jsonl("issues.jsonl"));
        assert!(!is_excluded_jsonl("beads.jsonl"));
        assert!(!is_excluded_jsonl("custom.jsonl"));
    }

    #[test]
    fn resolve_jsonl_uses_discovery_when_no_override() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Only legacy file exists
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");

        let metadata = Metadata::default();
        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);

        // Should discover beads.jsonl since issues.jsonl doesn't exist
        assert_eq!(resolved, beads_dir.join("beads.jsonl"));
    }

    #[test]
    fn resolve_jsonl_prefers_metadata_override() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Both legacy and custom exist
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");
        fs::write(beads_dir.join("custom.jsonl"), "{}").expect("write custom");

        let metadata = Metadata {
            database: DEFAULT_DB_FILENAME.to_string(),
            jsonl_export: "custom.jsonl".to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);
        assert_eq!(resolved, beads_dir.join("custom.jsonl"));
    }

    #[test]
    fn resolve_jsonl_ignores_excluded_metadata() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Create issues.jsonl
        fs::write(beads_dir.join("issues.jsonl"), "{}").expect("write issues");

        // Metadata points to excluded file (should be ignored)
        let metadata = Metadata {
            database: DEFAULT_DB_FILENAME.to_string(),
            jsonl_export: "deletions.jsonl".to_string(),
            backend: None,
            deletions_retention_days: None,
        };

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);
        // Should fall through to discovery, find issues.jsonl
        assert_eq!(resolved, beads_dir.join("issues.jsonl"));
    }

    #[test]
    fn resolve_jsonl_defaults_when_nothing_exists() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // No JSONL files exist
        let metadata = Metadata::default();
        let resolved = resolve_jsonl_path(&beads_dir, &metadata, None);

        // Should return default for writing
        assert_eq!(resolved, beads_dir.join("issues.jsonl"));
    }

    #[test]
    fn resolve_jsonl_db_override_derives_sibling() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let custom_dir = temp.path().join("custom");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        fs::create_dir_all(&custom_dir).expect("create custom dir");

        // Create files in beads_dir (should be ignored)
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");

        let metadata = Metadata::default();
        let db_override = custom_dir.join("custom.db");

        let resolved = resolve_jsonl_path(&beads_dir, &metadata, Some(&db_override));
        // Should derive sibling from db_override path
        assert_eq!(resolved, custom_dir.join("issues.jsonl"));
    }

    #[test]
    fn config_paths_uses_discovery() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Only legacy file exists
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");

        let paths = ConfigPaths::resolve(&beads_dir, None).expect("paths");

        // Should discover beads.jsonl
        assert_eq!(paths.jsonl_path, beads_dir.join("beads.jsonl"));
    }

    #[test]
    fn metadata_jsonl_override_respected() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Write metadata with custom jsonl_export
        fs::write(
            beads_dir.join("metadata.json"),
            r#"{"database": "beads.db", "jsonl_export": "my-export.jsonl"}"#,
        )
        .expect("write metadata");

        // Create the custom file
        fs::write(beads_dir.join("my-export.jsonl"), "{}").expect("write custom");

        let paths = ConfigPaths::resolve(&beads_dir, None).expect("paths");
        assert_eq!(paths.jsonl_path, beads_dir.join("my-export.jsonl"));
    }

    #[test]
    fn multiple_jsonl_candidates_prefers_issues() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        // Create multiple candidates
        fs::write(beads_dir.join("beads.jsonl"), "{}").expect("write beads");
        fs::write(beads_dir.join("issues.jsonl"), "{}").expect("write issues");
        fs::write(beads_dir.join("beads.base.jsonl"), "{}").expect("write base");
        fs::write(beads_dir.join("deletions.jsonl"), "{}").expect("write deletions");

        let paths = ConfigPaths::resolve(&beads_dir, None).expect("paths");

        // Should pick issues.jsonl (preferred over legacy, ignoring excluded)
        assert_eq!(paths.jsonl_path, beads_dir.join("issues.jsonl"));
    }

    #[test]
    fn should_attempt_jsonl_recovery_only_for_corruption_errors() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        fs::write(&db_path, b"sqlite bytes").expect("write db placeholder");
        fs::write(&jsonl_path, "{}\n").expect("write jsonl");

        assert!(should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::DatabaseCorrupt {
                detail: "bad page".to_string()
            }),
            &db_path,
            &jsonl_path
        ));
        assert!(should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::NotADatabase {
                path: db_path.clone()
            }),
            &db_path,
            &jsonl_path
        ));
        assert!(should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::WalCorrupt {
                detail: "bad wal".to_string()
            }),
            &db_path,
            &jsonl_path
        ));
        assert!(should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::ShortRead {
                expected: 4096,
                actual: 12
            }),
            &db_path,
            &jsonl_path
        ));

        assert!(!should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::SchemaChanged),
            &db_path,
            &jsonl_path
        ));
        assert!(!should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::CannotOpen {
                path: db_path.clone()
            }),
            &db_path,
            &jsonl_path
        ));
        assert!(!should_attempt_jsonl_recovery(
            &BeadsError::Database(FrankenError::Busy),
            &db_path,
            &jsonl_path
        ));
    }

    #[test]
    fn resolve_bootstrap_issue_prefix_prefers_bootstrap_layer() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        fs::write(&jsonl_path, "").expect("write empty jsonl");

        let mut bootstrap_layer = ConfigLayer::default();
        bootstrap_layer
            .runtime
            .insert("issue_prefix".to_string(), "cfg".to_string());

        let prefix = resolve_bootstrap_issue_prefix(&bootstrap_layer, &beads_dir, &jsonl_path)
            .expect("prefix");
        assert_eq!(prefix, "cfg");
    }

    #[test]
    fn open_storage_with_cli_recovers_corrupt_db_from_valid_jsonl() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        fs::write(&db_path, b"not a sqlite database").expect("write corrupt db");
        write_single_issue_jsonl(&jsonl_path, "bd-recover1", "Recovered from JSONL");

        let storage_ctx =
            open_storage_with_cli(&beads_dir, &CliOverrides::default()).expect("storage");
        let issue = storage_ctx
            .storage
            .get_issue("bd-recover1")
            .expect("query issue")
            .expect("issue should exist after recovery");

        assert_eq!(issue.title, "Recovered from JSONL");
        assert!(!storage_ctx.no_db);
        assert!(db_path.is_file(), "recovered database should exist");

        let recovery_dir = beads_dir.join(RECOVERY_DIR_NAME);
        let backups: Vec<_> = fs::read_dir(&recovery_dir)
            .expect("list recovery dir")
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            backups.iter().any(|name| {
                name.starts_with("beads.db.")
                    && Path::new(name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("bak"))
            }),
            "original database should be preserved in the recovery directory"
        );

        drop(storage_ctx);

        let reopened_ctx =
            open_storage_with_cli(&beads_dir, &CliOverrides::default()).expect("reopen storage");
        let reopened_issue = reopened_ctx
            .storage
            .get_issue("bd-recover1")
            .expect("query reopened issue")
            .expect("issue should remain readable after reopening");
        assert_eq!(reopened_issue.title, "Recovered from JSONL");
    }

    #[test]
    fn open_storage_with_cli_recovers_using_resolved_external_jsonl() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let external_dir = temp.path().join("external-store");
        let db_path = external_dir.join("beads.db");
        let jsonl_path = external_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        fs::create_dir_all(&external_dir).expect("create external dir");

        fs::write(&db_path, b"not a sqlite database").expect("write corrupt db");
        write_single_issue_jsonl(
            &jsonl_path,
            "bd-recover-external",
            "Recovered from external JSONL",
        );

        let cli = CliOverrides {
            db: Some(db_path),
            ..CliOverrides::default()
        };
        let storage_ctx = open_storage_with_cli(&beads_dir, &cli).expect("storage");
        let issue = storage_ctx
            .storage
            .get_issue("bd-recover-external")
            .expect("query issue")
            .expect("issue should exist after recovery");

        assert_eq!(issue.title, "Recovered from external JSONL");
        assert_eq!(storage_ctx.paths.jsonl_path, jsonl_path);
    }

    #[test]
    fn open_storage_with_cli_no_db_supports_external_jsonl() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let external_dir = temp.path().join("external-store");
        let db_path = external_dir.join("beads.db");
        let jsonl_path = external_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        fs::create_dir_all(&external_dir).expect("create external dir");

        write_single_issue_jsonl(&jsonl_path, "bd-ext-import", "Imported from external JSONL");

        let cli = CliOverrides {
            db: Some(db_path),
            no_db: Some(true),
            ..CliOverrides::default()
        };
        let mut storage_ctx = open_storage_with_cli(&beads_dir, &cli).expect("storage");
        let imported = storage_ctx
            .storage
            .get_issue("bd-ext-import")
            .expect("query imported issue")
            .expect("issue should be imported");
        assert_eq!(imported.title, "Imported from external JSONL");

        let new_issue = Issue {
            id: "bd-ext-flush".to_string(),
            title: "Flushed to external JSONL".to_string(),
            ..Issue::default()
        };
        storage_ctx
            .storage
            .create_issue(&new_issue, "tester")
            .expect("create issue");
        storage_ctx
            .flush_no_db_if_dirty()
            .expect("flush no-db export");

        let exported = fs::read_to_string(&jsonl_path).expect("read external jsonl");
        assert!(
            exported.contains("\"id\":\"bd-ext-flush\""),
            "flush should export to the resolved external JSONL path"
        );
    }

    #[test]
    fn open_storage_with_cli_backs_up_non_file_sidecars_that_block_recovery() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        let wal_dir = beads_dir.join("beads.db-wal");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::create_dir_all(&wal_dir).expect("create fake wal dir");

        fs::write(&db_path, b"not a sqlite database").expect("write corrupt db");
        write_single_issue_jsonl(&jsonl_path, "bd-recover2", "Recovered with odd sidecar");
        fs::write(wal_dir.join("sentinel.txt"), "keep me").expect("write sentinel");

        let storage_ctx =
            open_storage_with_cli(&beads_dir, &CliOverrides::default()).expect("storage");
        let issue = storage_ctx
            .storage
            .get_issue("bd-recover2")
            .expect("query issue")
            .expect("issue should exist after recovery");

        assert_eq!(issue.title, "Recovered with odd sidecar");
        assert!(
            !wal_dir.join("sentinel.txt").exists(),
            "the original blocking wal directory should be moved away rather than reused in place"
        );

        let recovery_dir = beads_dir.join(RECOVERY_DIR_NAME);
        let wal_backups: Vec<_> = fs::read_dir(&recovery_dir)
            .expect("list recovery dir")
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("beads.db-wal.")
                            && Path::new(name)
                                .extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("bak"))
                    })
            })
            .collect();
        assert_eq!(
            wal_backups.len(),
            1,
            "wal directory should be backed up once"
        );
        assert_eq!(
            fs::read_to_string(wal_backups[0].join("sentinel.txt"))
                .expect("read backed-up sentinel"),
            "keep me"
        );
    }

    #[test]
    fn open_storage_with_cli_does_not_recover_from_invalid_jsonl() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        fs::write(&db_path, b"not a sqlite database").expect("write corrupt db");
        fs::write(&jsonl_path, "{not valid json\n").expect("write invalid jsonl");

        let err =
            open_storage_with_cli(&beads_dir, &CliOverrides::default()).expect_err("should fail");
        assert!(
            matches!(err, BeadsError::Database(_)),
            "invalid JSONL should preserve the original database open error"
        );
        assert!(
            db_path.is_file(),
            "original database should remain in place"
        );

        let recovery_dir = beads_dir.join(RECOVERY_DIR_NAME);
        let backup_count =
            fs::read_dir(&recovery_dir).map_or(0, |entries| entries.flatten().count());
        assert_eq!(
            backup_count, 0,
            "no recovery backup should be created when JSONL preflight fails"
        );
    }

    #[test]
    fn move_database_family_to_recovery_rolls_back_partial_failure() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        let db_path = beads_dir.join("beads.db");
        let wal_path = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        fs::create_dir_all(&beads_dir).expect("create beads dir");

        fs::write(&db_path, b"db").expect("write db");
        fs::write(&wal_path, b"wal").expect("write wal");

        let recovery_dir = recovery_dir_for_db_path(&db_path, &beads_dir);
        fs::create_dir_all(&recovery_dir).expect("create recovery dir");

        let stamp = "fixed-stamp";
        let conflicting_wal_backup =
            recovery_dir.join(recovery_backup_filename(&wal_path, stamp, "bak"));
        fs::create_dir_all(&conflicting_wal_backup).expect("create conflicting wal backup dir");

        let err =
            move_database_family_to_recovery(&db_path, &beads_dir, stamp).expect_err("should fail");
        assert!(matches!(err, BeadsError::Io(_)));

        assert!(db_path.is_file(), "db should be restored after rollback");
        assert!(wal_path.is_file(), "wal should remain after rollback");

        let db_backup = recovery_dir.join(recovery_backup_filename(&db_path, stamp, "bak"));
        assert!(
            !db_backup.exists(),
            "rolled back db backup should not remain in recovery dir"
        );
        assert!(
            conflicting_wal_backup.is_dir(),
            "the pre-existing conflicting path should be untouched"
        );
    }
}
