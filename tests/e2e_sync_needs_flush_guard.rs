//! E2E regression tests for issue #405 (upstream beabee6d), ported to this fork: the
//! internal `needs_flush` marker must not stand in for the user's `--force`, because
//! that turns off the exporter's data-loss guards ("Refusing to export empty
//! database..." / "Refusing to export stale database...").
//!
//! In this fork only a hard delete (`delete --hard`, the purge) sets `needs_flush`.
//! Before the port, the flush that followed a hard delete ran with `force`, so it
//! rewrote `issues.jsonl` from the database and erased every issue the database had
//! not imported. The purge itself is now recorded as purged-pending-export, which the
//! guards leave out of their loss check.

mod common;

use common::cli::{BrWorkspace, run_br};
use serde_json::Value;
use std::fs;
use std::path::Path;

fn parse_created_id(stdout: &str) -> String {
    let line = stdout.lines().next().unwrap_or("");
    let normalized = line.strip_prefix("✓ ").unwrap_or(line);
    normalized
        .strip_prefix("Created ")
        .and_then(|rest| rest.split(':').next())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn jsonl_ids(issues_path: &Path) -> Vec<String> {
    fs::read_to_string(issues_path)
        .expect("read jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let record: Value = serde_json::from_str(line).expect("parse jsonl line");
            record["id"].as_str().expect("id").to_string()
        })
        .collect()
}

const MERGED_ISSUE_LINE: &str = "{\"id\":\"bd-merged1\",\"title\":\"Merged from another machine\",\"status\":\"open\",\"priority\":2,\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:00:00Z\",\"updated_at\":\"2026-01-01T00:00:00Z\"}\n";

/// A workspace with a keeper and a victim, both flushed to `issues.jsonl`.
fn seeded_workspace() -> (BrWorkspace, std::path::PathBuf, String) {
    let workspace = BrWorkspace::new();
    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);
    let issues_path = workspace.root.join(".beads").join("issues.jsonl");

    let keep = run_br(
        &workspace,
        ["create", "Keeper", "--no-auto-flush", "--no-auto-import"],
        "create_keeper",
    );
    assert!(keep.status.success(), "create failed: {}", keep.stderr);
    let victim = run_br(
        &workspace,
        ["create", "Victim", "--no-auto-flush", "--no-auto-import"],
        "create_victim",
    );
    assert!(victim.status.success(), "create failed: {}", victim.stderr);
    let victim_id = parse_created_id(&victim.stdout);
    assert!(!victim_id.is_empty(), "no id in: {}", victim.stdout);

    let flush = run_br(
        &workspace,
        ["sync", "--flush-only", "--no-auto-import"],
        "seed_flush",
    );
    assert!(
        flush.status.success(),
        "seed flush failed: {}",
        flush.stderr
    );
    assert_eq!(jsonl_ids(&issues_path).len(), 2);
    (workspace, issues_path, victim_id)
}

fn hard_delete(workspace: &BrWorkspace, id: &str) {
    let del = run_br(
        workspace,
        [
            "delete",
            id,
            "--force",
            "--no-auto-flush",
            "--no-auto-import",
        ],
        "soft_delete",
    );
    assert!(del.status.success(), "delete failed: {}", del.stderr);
    let purge = run_br(
        workspace,
        [
            "delete",
            id,
            "--hard",
            "--force",
            "--no-auto-flush",
            "--no-auto-import",
        ],
        "hard_delete",
    );
    assert!(
        purge.status.success(),
        "hard delete failed: {}",
        purge.stderr
    );
}

/// A hard delete, then a flush, then a count: an issue the JSONL holds and the
/// database never imported stays in the JSONL, and the flush refuses, naming it.
#[test]
fn e2e_flush_after_hard_delete_does_not_erase_unimported_issues() {
    let (workspace, issues_path, victim_id) = seeded_workspace();

    // `git pull` merged an issue this database has not imported yet.
    let mut contents = fs::read_to_string(&issues_path).expect("read jsonl");
    contents.push_str(MERGED_ISSUE_LINE);
    fs::write(&issues_path, contents).expect("append merged issue");
    assert_eq!(jsonl_ids(&issues_path).len(), 3);

    hard_delete(&workspace, &victim_id);
    let flush = run_br(
        &workspace,
        ["sync", "--flush-only", "--no-auto-import"],
        "post_purge_flush",
    );

    let ids = jsonl_ids(&issues_path);
    assert_eq!(ids.len(), 3, "the flush dropped JSONL issues: {ids:?}");
    assert!(
        ids.iter().any(|id| id == "bd-merged1"),
        "bd-merged1 was erased: {ids:?}"
    );
    assert!(
        !flush.status.success(),
        "the flush must refuse while the JSONL holds an issue the database lacks; stdout: {} stderr: {}",
        flush.stdout,
        flush.stderr
    );
    assert!(
        flush.stderr.contains("Refusing to export stale database"),
        "expected the stale-database guard, got stderr: {}",
        flush.stderr
    );
    assert!(
        flush.stderr.contains("bd-merged1"),
        "the guard names the would-be-lost issue; stderr: {}",
        flush.stderr
    );
}

/// The flow the old conflation served: a hard delete, then a flush without `--force`,
/// prunes exactly the purged issue.
#[test]
fn e2e_hard_delete_flush_still_prunes_purged_issues() {
    let (workspace, issues_path, victim_id) = seeded_workspace();

    hard_delete(&workspace, &victim_id);
    let flush = run_br(
        &workspace,
        ["sync", "--flush-only", "--no-auto-import"],
        "post_purge_flush",
    );
    assert!(
        flush.status.success(),
        "flush after purge must succeed without --force; stderr: {}",
        flush.stderr
    );

    let ids = jsonl_ids(&issues_path);
    assert_eq!(
        ids.len(),
        1,
        "exactly the purged issue leaves the JSONL: {ids:?}"
    );
    assert!(
        !ids.contains(&victim_id),
        "purged issue must be pruned from JSONL"
    );
    let jsonl = fs::read_to_string(&issues_path).expect("read jsonl");
    assert!(
        jsonl.contains("Keeper"),
        "keeper issue must survive the post-purge flush"
    );
}
