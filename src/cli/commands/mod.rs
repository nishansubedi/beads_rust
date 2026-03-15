use crate::config::OpenStorageResult;
use crate::error::BeadsError;
use crate::model::Issue;
use crate::storage::{IssueUpdate, SqliteStorage};
use crate::util::id::{IdResolver, find_matching_ids};

pub mod agents;
pub mod audit;
pub mod blocked;
pub mod changelog;
pub mod close;
pub mod comments;
pub mod completions;
pub mod config;
pub mod count;
pub mod create;
pub mod defer;
pub mod delete;
pub mod dep;
pub mod doctor;
pub mod epic;
pub mod graph;
pub mod history;
pub mod info;
pub mod init;
pub mod label;
pub mod lint;
pub mod list;
pub mod orphans;
pub mod q;
pub mod query;
pub mod ready;
pub mod reopen;
pub mod schema;
pub mod search;
pub mod show;
pub mod stale;
pub mod stats;
pub mod sync;
pub mod update;
pub mod version;
pub mod r#where;

#[cfg(feature = "self_update")]
pub mod upgrade;

/// Resolve an issue ID from a potentially partial input.
pub(super) fn resolve_issue_id(
    storage: &SqliteStorage,
    resolver: &IdResolver,
    all_ids: &[String],
    input: &str,
) -> crate::Result<String> {
    resolver
        .resolve_fallible(
            input,
            |id| storage.id_exists(id),
            |hash| Ok(find_matching_ids(all_ids, hash)),
        )
        .map(|resolved| resolved.id)
}

pub(super) fn rebuild_blocked_cache_after_partial_mutation(
    storage: &mut SqliteStorage,
    cache_dirty: bool,
    command: &str,
) -> crate::Result<()> {
    if !cache_dirty {
        return Ok(());
    }

    storage
        .rebuild_blocked_cache(true)
        .map(|_| ())
        .map_err(|rebuild_err| crate::error::BeadsError::WithContext {
            context: format!("failed to rebuild blocked cache after partial {command} mutation"),
            source: Box::new(rebuild_err),
        })
}

pub(super) fn preserve_blocked_cache_on_error<T>(
    storage: &mut SqliteStorage,
    cache_dirty: bool,
    command: &str,
    result: crate::Result<T>,
) -> crate::Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(operation_err) => {
            if let Err(rebuild_err) =
                rebuild_blocked_cache_after_partial_mutation(storage, cache_dirty, command)
            {
                return Err(crate::error::BeadsError::WithContext {
                    context: format!(
                        "failed to preserve blocked cache after partial {command} mutation; original operation error: {operation_err}"
                    ),
                    source: Box::new(rebuild_err),
                });
            }
            Err(operation_err)
        }
    }
}

pub(super) fn update_issue_with_recovery(
    storage_ctx: &mut OpenStorageResult,
    allow_recovery: bool,
    command: &str,
    issue_id: &str,
    update: &IssueUpdate,
    actor: &str,
) -> crate::Result<Issue> {
    retry_mutation_with_jsonl_recovery(
        storage_ctx,
        allow_recovery,
        command,
        Some(issue_id),
        |storage| storage.update_issue(issue_id, update, actor),
    )
}

pub(super) fn retry_mutation_with_jsonl_recovery<T, F>(
    storage_ctx: &mut OpenStorageResult,
    allow_recovery: bool,
    command: &str,
    probe_issue_id: Option<&str>,
    mut operation: F,
) -> crate::Result<T>
where
    F: FnMut(&mut SqliteStorage) -> crate::Result<T>,
{
    match operation(&mut storage_ctx.storage) {
        Ok(value) => Ok(value),
        Err(operation_err) => {
            if !allow_recovery || !matches!(operation_err, BeadsError::Database(_)) {
                return Err(operation_err);
            }

            let mut recovery_signal = storage_ctx.should_attempt_jsonl_recovery(&operation_err);
            let mut probe_error: Option<BeadsError> = None;

            if !recovery_signal && let Some(issue_id) = probe_issue_id {
                match storage_ctx
                    .storage
                    .probe_issue_mutation_write_path(issue_id)
                {
                    Ok(()) => return Err(operation_err),
                    Err(probe_err) => {
                        recovery_signal = storage_ctx.should_attempt_jsonl_recovery(&probe_err);
                        probe_error = Some(probe_err);
                    }
                }
            }

            if !recovery_signal {
                return Err(operation_err);
            }

            let issue_id_label = probe_issue_id.unwrap_or("<none>");
            let probe_error_display = probe_error
                .as_ref()
                .map_or_else(|| "n/a".to_string(), std::string::ToString::to_string);
            tracing::warn!(
                command = command,
                issue_id = issue_id_label,
                original_error = %operation_err,
                probe_error = %probe_error_display,
                db_path = %storage_ctx.paths.db_path.display(),
                jsonl_path = %storage_ctx.paths.jsonl_path.display(),
                "Mutation hit a recoverable database corruption path; rebuilding from JSONL and retrying once"
            );

            let original_error = operation_err.to_string();
            storage_ctx.recover_database_from_jsonl().map_err(|recovery_err| {
                BeadsError::WithContext {
                    context: probe_issue_id.map_or_else(
                        || {
                            format!(
                                "automatic database recovery failed after {command} write; original write error: {original_error}"
                            )
                        },
                        |issue_id| {
                        format!(
                            "automatic database recovery failed after {command} write for issue '{issue_id}'; original write error: {original_error}"
                        )
                        },
                    ),
                    source: Box::new(recovery_err),
                }
            })?;

            operation(&mut storage_ctx.storage)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        preserve_blocked_cache_on_error, rebuild_blocked_cache_after_partial_mutation,
        retry_mutation_with_jsonl_recovery,
    };
    use crate::config::{CliOverrides, open_storage_with_cli};
    use crate::error::BeadsError;
    use crate::model::Issue;
    use crate::storage::SqliteStorage;
    use crate::sync::{ExportConfig, export_to_jsonl_with_policy};
    use crate::storage::compat::Connection;
    use crate::storage::compat::CompatError as FrankenError;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn partial_mutation_rebuild_skips_clean_state() {
        let mut storage = SqliteStorage::open_memory().expect("storage");
        rebuild_blocked_cache_after_partial_mutation(&mut storage, false, "close")
            .expect("clean state should not rebuild");
    }

    #[test]
    fn preserve_returns_original_error_when_rebuild_succeeds() {
        let mut storage = SqliteStorage::open_memory().expect("storage");
        let result: crate::Result<()> = Err(BeadsError::validation("ids", "boom"));
        let err = preserve_blocked_cache_on_error::<()>(&mut storage, true, "close", result)
            .expect_err("operation should still fail");

        assert!(matches!(err, BeadsError::Validation { .. }));
    }

    #[test]
    fn preserve_surfaces_rebuild_failure() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("beads.db");
        let mut storage = SqliteStorage::open(&db_path).expect("storage");
        let conn = Connection::open(db_path.to_string_lossy().into_owned()).expect("conn");
        conn.execute("DROP TABLE blocked_issues_cache")
            .expect("drop blocked cache table");

        let result: crate::Result<()> = Err(BeadsError::validation("ids", "boom"));
        let err = preserve_blocked_cache_on_error::<()>(&mut storage, true, "reopen", result)
            .expect_err("rebuild failure should be surfaced");

        match err {
            BeadsError::WithContext { context, .. } => {
                assert!(context.contains("partial reopen mutation"));
                assert!(context.contains("Validation failed: ids: boom"));
            }
            other => panic!("expected WithContext, got {other:?}"),
        }
    }

    #[test]
    fn retry_mutation_recovers_from_recoverable_database_error() {
        let temp = TempDir::new().expect("tempdir");
        let beads_dir = temp.path().join(".beads");
        fs::create_dir_all(&beads_dir).expect("create beads dir");
        let db_path = beads_dir.join("beads.db");
        let jsonl_path = beads_dir.join("issues.jsonl");

        let mut storage = SqliteStorage::open(&db_path).expect("storage");
        let issue = Issue {
            id: "bd-1".to_string(),
            title: "test".to_string(),
            ..Issue::default()
        };
        storage
            .create_issue(&issue, "tester")
            .expect("create issue");
        let export_config = ExportConfig {
            beads_dir: Some(beads_dir.clone()),
            ..Default::default()
        };
        export_to_jsonl_with_policy(&storage, &jsonl_path, &export_config).expect("export jsonl");

        let mut storage_ctx =
            open_storage_with_cli(&beads_dir, &CliOverrides::default()).expect("storage ctx");
        let mut attempts = 0;

        let result = retry_mutation_with_jsonl_recovery(
            &mut storage_ctx,
            true,
            "test-mutation",
            Some("bd-1"),
            |_storage| {
                attempts += 1;
                if attempts == 1 {
                    Err(BeadsError::Database(FrankenError::DatabaseCorrupt {
                        detail: "synthetic corruption".to_string(),
                    }))
                } else {
                    Ok("recovered")
                }
            },
        )
        .expect("recovered mutation");

        assert_eq!(result, "recovered");
        assert_eq!(attempts, 2);
        assert!(
            storage_ctx
                .storage
                .get_issue("bd-1")
                .expect("load issue")
                .is_some()
        );
    }
}
