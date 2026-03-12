mod common;

use common::cli::{BrRun, BrWorkspace, extract_json_payload, run_br};
use common::{
    WorkspaceFailureCommandOutcome, WorkspaceFailureFixtureMetadata,
    isolated_workspace_failure_fixture, list_workspace_failure_fixtures,
};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};

struct FixtureWorkspace {
    metadata: WorkspaceFailureFixtureMetadata,
    beads_dir: PathBuf,
    workspace: BrWorkspace,
}

fn fixture_workspace(name: &str) -> FixtureWorkspace {
    let isolated = isolated_workspace_failure_fixture(name).expect("isolated fixture");
    let metadata = isolated.fixture.metadata.clone();
    let root = isolated.root.clone();
    let beads_dir = isolated.beads_dir.clone();
    let log_dir = root.join("logs");
    fs::create_dir_all(&log_dir).expect("log dir");

    FixtureWorkspace {
        metadata,
        beads_dir,
        workspace: BrWorkspace {
            temp_dir: isolated.temp_dir,
            root,
            log_dir,
        },
    }
}

fn parse_stdout_json(run: &BrRun, context: &str) -> Value {
    let payload = extract_json_payload(&run.stdout);
    serde_json::from_str(&payload).unwrap_or_else(|err| {
        panic!(
            "{context} should emit valid JSON on stdout: {err}\nstdout={}\nstderr={}",
            run.stdout, run.stderr
        )
    })
}

fn parse_stderr_json(run: &BrRun, context: &str) -> Value {
    let payload = extract_json_payload(&run.stderr);
    serde_json::from_str(&payload).unwrap_or_else(|err| {
        panic!(
            "{context} should emit structured JSON on stderr: {err}\nstdout={}\nstderr={}",
            run.stdout, run.stderr
        )
    })
}

fn doctor_check<'a>(doctor_json: &'a Value, name: &str) -> &'a Value {
    doctor_json["checks"]
        .as_array()
        .and_then(|checks| checks.iter().find(|check| check["name"] == name))
        .unwrap_or_else(|| panic!("doctor report missing check '{name}': {doctor_json}"))
}

fn surface_label(name: &str, surface: &str) -> String {
    let slug: String = surface
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect();
    format!("{name}_{slug}")
}

fn run_surface(fixture: &FixtureWorkspace, surface: &str) -> BrRun {
    let label = surface_label(&fixture.metadata.name, surface);
    match surface {
        "startup/open" => run_br(&fixture.workspace, ["list", "--json"], &label),
        "create" => run_br(
            &fixture.workspace,
            ["create", "Replay harness probe", "--json"],
            &label,
        ),
        "doctor" => run_br(&fixture.workspace, ["doctor", "--json"], &label),
        "doctor --repair" => run_br(&fixture.workspace, ["doctor", "--repair", "--json"], &label),
        "sync --status" => run_br(&fixture.workspace, ["sync", "--status", "--json"], &label),
        "sync --import-only" => run_br(
            &fixture.workspace,
            ["sync", "--import-only", "--json"],
            &label,
        ),
        "list --no-db" => run_br(&fixture.workspace, ["--no-db", "list", "--json"], &label),
        "config get" => run_br(
            &fixture.workspace,
            ["config", "get", "issue_prefix", "--json"],
            &label,
        ),
        "config list" => run_br(&fixture.workspace, ["config", "list", "--json"], &label),
        "history" => run_br(&fixture.workspace, ["history", "list", "--json"], &label),
        "where" => run_br(&fixture.workspace, ["where", "--json"], &label),
        "info" => run_br(&fixture.workspace, ["info", "--json"], &label),
        other => panic!("unsupported replay surface '{other}'"),
    }
}

fn assert_sqlite_header(db_path: &Path, context: &str) {
    let bytes = fs::read(db_path).unwrap_or_else(|err| {
        panic!(
            "{context} should leave a readable SQLite database at {}: {err}",
            db_path.display()
        )
    });
    assert!(
        bytes.starts_with(b"SQLite format 3\0"),
        "{context} should leave a SQLite database header at {}",
        db_path.display()
    );
}

fn assert_config_error(run: &BrRun, needle: &str, context: &str) {
    assert!(
        !run.status.success(),
        "{context} should fail\nstdout={}\nstderr={}",
        run.stdout,
        run.stderr
    );
    let error_json = parse_stderr_json(run, context);
    assert_eq!(
        error_json["error"]["code"].as_str(),
        Some("CONFIG_ERROR"),
        "{context} should surface CONFIG_ERROR: {error_json}"
    );
    assert!(
        error_json["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains(needle)),
        "{context} should mention '{needle}': {error_json}"
    );
}

fn first_issue_id(list_json: &Value) -> String {
    list_json
        .as_array()
        .and_then(|issues| issues.first())
        .and_then(|issue| issue["id"].as_str())
        .map(str::to_string)
        .expect("list output should contain at least one issue id")
}

fn first_issue_id_from_jsonl(jsonl_path: &Path) -> String {
    let contents = fs::read_to_string(jsonl_path).expect("read jsonl");
    contents
        .lines()
        .find_map(|line| serde_json::from_str::<Value>(line).ok())
        .and_then(|issue| issue["id"].as_str().map(str::to_string))
        .expect("fixture jsonl should contain at least one valid issue id")
}

fn create_issue_id(create_json: &Value) -> String {
    if let Some(created) = create_json["created"]
        .as_array()
        .and_then(|created| created.first())
    {
        return created["id"]
            .as_str()
            .map(str::to_string)
            .expect("created entry should contain id");
    }
    create_json["id"]
        .as_str()
        .map(str::to_string)
        .expect("create output should contain id")
}

fn assert_surface_outcome(
    fixture: &FixtureWorkspace,
    surface: &str,
    outcome: WorkspaceFailureCommandOutcome,
) {
    let run = run_surface(fixture, surface);
    let context = format!("{} {surface}", fixture.metadata.name);

    match outcome {
        WorkspaceFailureCommandOutcome::Success => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            if fixture.metadata.name == "metadata_custom_paths" && surface == "where" {
                assert!(
                    json["database_path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/custom.db")),
                    "where should resolve custom database path: {json}"
                );
                assert!(
                    json["jsonl_path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/custom.jsonl")),
                    "where should resolve custom JSONL path: {json}"
                );
            }
            if fixture.metadata.name == "metadata_custom_paths" && surface == "info" {
                assert!(
                    json["database_path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/custom.db")),
                    "info should resolve custom database path: {json}"
                );
                assert!(
                    json["jsonl_path"]
                        .as_str()
                        .is_some_and(|path| path.ends_with("/custom.jsonl")),
                    "info should resolve custom JSONL path: {json}"
                );
            }
        }
        WorkspaceFailureCommandOutcome::SuccessWithAutoRecovery => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let _json = parse_stdout_json(&run, &context);
            assert_sqlite_header(&fixture.beads_dir.join("beads.db"), &context);
        }
        WorkspaceFailureCommandOutcome::DoctorClean => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["ok"],
                Value::Bool(true),
                "{context} should be clean: {json}"
            );
            if fixture.metadata.name == "db_jsonl_disagreement" {
                let counts = doctor_check(&json, "counts.db_vs_jsonl");
                assert_eq!(
                    counts["status"].as_str(),
                    Some("warn"),
                    "db_jsonl_disagreement should warn on DB/JSONL drift: {json}"
                );
            }
        }
        WorkspaceFailureCommandOutcome::ReportsErrors => {
            assert!(
                !run.status.success(),
                "{context} should report errors\nstdout={}\nstderr={}",
                run.stdout,
                run.stderr
            );
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["ok"],
                Value::Bool(false),
                "{context} should be unhealthy: {json}"
            );
        }
        WorkspaceFailureCommandOutcome::RepairApplied => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["repaired"],
                Value::Bool(true),
                "{context} should apply repair: {json}"
            );
            assert_eq!(
                json["verified"],
                Value::Bool(true),
                "{context} should verify the repair: {json}"
            );
            assert_eq!(
                json["post_repair"]["ok"],
                Value::Bool(true),
                "{context} should leave the workspace healthy: {json}"
            );
        }
        WorkspaceFailureCommandOutcome::RepairNoop => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["repaired"],
                Value::Bool(false),
                "{context} should report a repair noop: {json}"
            );
        }
        WorkspaceFailureCommandOutcome::StatusInSync => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["jsonl_newer"],
                Value::Bool(false),
                "{context} should not report newer JSONL: {json}"
            );
            assert_eq!(
                json["db_newer"],
                Value::Bool(false),
                "{context} should not report newer DB state: {json}"
            );
        }
        WorkspaceFailureCommandOutcome::StatusJsonlNewer => {
            assert!(run.status.success(), "{context} failed: {}", run.stderr);
            let json = parse_stdout_json(&run, &context);
            assert_eq!(
                json["jsonl_newer"],
                Value::Bool(true),
                "{context} should report newer JSONL: {json}"
            );
            assert_eq!(
                json["db_newer"],
                Value::Bool(false),
                "{context} should not report newer DB state: {json}"
            );
        }
        WorkspaceFailureCommandOutcome::FailsPrefixMismatch => {
            assert_config_error(&run, "Prefix mismatch", &context);
        }
        WorkspaceFailureCommandOutcome::FailsConflictMarkers => {
            assert_config_error(&run, "Merge conflict markers detected", &context);
        }
        WorkspaceFailureCommandOutcome::FailsInvalidJson => {
            assert_config_error(&run, "Invalid JSON", &context);
        }
    }
}

#[test]
fn workspace_failure_replay_manifest_expectations_hold_on_fresh_copies() {
    let _log =
        common::test_log("workspace_failure_replay_manifest_expectations_hold_on_fresh_copies");
    let fixtures = list_workspace_failure_fixtures().expect("fixture catalog");

    for fixture in fixtures {
        for expectation in &fixture.metadata.expected_command_outcomes {
            let workspace = fixture_workspace(&fixture.metadata.name);
            assert_surface_outcome(&workspace, &expectation.surface, expectation.outcome);
        }
    }
}

#[test]
fn workspace_failure_replay_core_read_surfaces_match_expected_posture() {
    let _log =
        common::test_log("workspace_failure_replay_core_read_surfaces_match_expected_posture");
    let fixtures = list_workspace_failure_fixtures().expect("fixture catalog");

    for fixture in fixtures {
        let where_workspace = fixture_workspace(&fixture.metadata.name);
        let where_run = run_br(
            &where_workspace.workspace,
            ["where", "--json"],
            &surface_label(&fixture.metadata.name, "core_where"),
        );
        assert!(
            where_run.status.success(),
            "{} where --json failed: {}",
            fixture.metadata.name,
            where_run.stderr
        );
        let where_json =
            parse_stdout_json(&where_run, &format!("{} core where", fixture.metadata.name));

        let info_workspace = fixture_workspace(&fixture.metadata.name);
        let info = run_br(
            &info_workspace.workspace,
            ["info", "--json"],
            &surface_label(&fixture.metadata.name, "core_info"),
        );
        assert!(
            info.status.success(),
            "{} info --json failed: {}",
            fixture.metadata.name,
            info.stderr
        );
        let _info_json = parse_stdout_json(&info, &format!("{} core info", fixture.metadata.name));

        match fixture
            .metadata
            .outcome_for("startup/open")
            .expect("startup/open expectation")
        {
            WorkspaceFailureCommandOutcome::Success
            | WorkspaceFailureCommandOutcome::SuccessWithAutoRecovery => {
                let list_workspace = fixture_workspace(&fixture.metadata.name);
                let list = run_br(
                    &list_workspace.workspace,
                    ["list", "--json"],
                    &surface_label(&fixture.metadata.name, "core_list"),
                );
                assert!(
                    list.status.success(),
                    "{} list --json failed: {}",
                    fixture.metadata.name,
                    list.stderr
                );
                let list_json =
                    parse_stdout_json(&list, &format!("{} core list", fixture.metadata.name));
                let issue_id = first_issue_id(&list_json);

                let ready_workspace = fixture_workspace(&fixture.metadata.name);
                let ready = run_br(
                    &ready_workspace.workspace,
                    ["ready", "--json"],
                    &surface_label(&fixture.metadata.name, "core_ready"),
                );
                assert!(
                    ready.status.success(),
                    "{} ready --json failed: {}",
                    fixture.metadata.name,
                    ready.stderr
                );
                let _ready_json =
                    parse_stdout_json(&ready, &format!("{} core ready", fixture.metadata.name));

                let show_workspace = fixture_workspace(&fixture.metadata.name);
                let show = run_br(
                    &show_workspace.workspace,
                    ["show", &issue_id, "--json"],
                    &surface_label(&fixture.metadata.name, "core_show"),
                );
                assert!(
                    show.status.success(),
                    "{} show --json failed: {}",
                    fixture.metadata.name,
                    show.stderr
                );
                let _show_json =
                    parse_stdout_json(&show, &format!("{} core show", fixture.metadata.name));
                if fixture
                    .metadata
                    .outcome_for("startup/open")
                    .is_some_and(|outcome| {
                        outcome == WorkspaceFailureCommandOutcome::SuccessWithAutoRecovery
                    })
                {
                    assert_sqlite_header(
                        &show_workspace.beads_dir.join("beads.db"),
                        &format!("{} core show", fixture.metadata.name),
                    );
                }
            }
            WorkspaceFailureCommandOutcome::FailsPrefixMismatch
            | WorkspaceFailureCommandOutcome::FailsConflictMarkers => {
                let failure = fixture
                    .metadata
                    .outcome_for("startup/open")
                    .expect("startup/open failure");
                let list_workspace = fixture_workspace(&fixture.metadata.name);
                assert_surface_outcome(&list_workspace, "startup/open", failure);

                let ready_workspace = fixture_workspace(&fixture.metadata.name);
                let ready = run_br(
                    &ready_workspace.workspace,
                    ["ready", "--json"],
                    &surface_label(&fixture.metadata.name, "core_ready_fail"),
                );
                match failure {
                    WorkspaceFailureCommandOutcome::FailsPrefixMismatch => {
                        assert_config_error(
                            &ready,
                            "Prefix mismatch",
                            &format!("{} core ready", fixture.metadata.name),
                        );
                    }
                    WorkspaceFailureCommandOutcome::FailsConflictMarkers => {
                        assert_config_error(
                            &ready,
                            "Merge conflict markers detected",
                            &format!("{} core ready", fixture.metadata.name),
                        );
                    }
                    _ => unreachable!(),
                }

                let jsonl_path = where_json["jsonl_path"]
                    .as_str()
                    .map(PathBuf::from)
                    .expect("where jsonl_path");
                let issue_id = first_issue_id_from_jsonl(&jsonl_path);
                let show_workspace = fixture_workspace(&fixture.metadata.name);
                let show = run_br(
                    &show_workspace.workspace,
                    ["show", &issue_id, "--json"],
                    &surface_label(&fixture.metadata.name, "core_show_fail"),
                );
                match failure {
                    WorkspaceFailureCommandOutcome::FailsPrefixMismatch => {
                        assert_config_error(
                            &show,
                            "Prefix mismatch",
                            &format!("{} core show", fixture.metadata.name),
                        );
                    }
                    WorkspaceFailureCommandOutcome::FailsConflictMarkers => {
                        assert_config_error(
                            &show,
                            "Merge conflict markers detected",
                            &format!("{} core show", fixture.metadata.name),
                        );
                    }
                    _ => unreachable!(),
                }
            }
            other => panic!(
                "{} has unsupported startup/open outcome for core read replay: {:?}",
                fixture.metadata.name, other
            ),
        }
    }
}

#[test]
fn workspace_failure_replay_core_write_surfaces_match_expected_posture() {
    let _log =
        common::test_log("workspace_failure_replay_core_write_surfaces_match_expected_posture");
    let fixtures = list_workspace_failure_fixtures().expect("fixture catalog");

    for fixture in fixtures {
        let expected_create = fixture
            .metadata
            .outcome_for("create")
            .expect("create expectation");
        let workspace = fixture_workspace(&fixture.metadata.name);
        let create = run_br(
            &workspace.workspace,
            ["create", "Replay write probe", "--json"],
            &surface_label(&fixture.metadata.name, "core_create"),
        );

        match expected_create {
            WorkspaceFailureCommandOutcome::Success
            | WorkspaceFailureCommandOutcome::SuccessWithAutoRecovery => {
                assert!(
                    create.status.success(),
                    "{} create failed: {}",
                    fixture.metadata.name,
                    create.stderr
                );
                let create_json =
                    parse_stdout_json(&create, &format!("{} core create", fixture.metadata.name));
                let issue_id = create_issue_id(&create_json);
                if expected_create == WorkspaceFailureCommandOutcome::SuccessWithAutoRecovery {
                    assert_sqlite_header(
                        &workspace.beads_dir.join("beads.db"),
                        &format!("{} core create", fixture.metadata.name),
                    );
                }

                let show = run_br(
                    &workspace.workspace,
                    ["show", &issue_id, "--json"],
                    &surface_label(&fixture.metadata.name, "core_show_created"),
                );
                assert!(
                    show.status.success(),
                    "{} show after create failed: {}",
                    fixture.metadata.name,
                    show.stderr
                );
                let _show_json = parse_stdout_json(
                    &show,
                    &format!("{} core show after create", fixture.metadata.name),
                );

                let update = run_br(
                    &workspace.workspace,
                    ["update", &issue_id, "--status", "in_progress", "--json"],
                    &surface_label(&fixture.metadata.name, "core_update"),
                );
                assert!(
                    update.status.success(),
                    "{} update failed: {}",
                    fixture.metadata.name,
                    update.stderr
                );

                let label_add = run_br(
                    &workspace.workspace,
                    ["label", "add", &issue_id, "replay-probe", "--json"],
                    &surface_label(&fixture.metadata.name, "core_label"),
                );
                assert!(
                    label_add.status.success(),
                    "{} label add failed: {}",
                    fixture.metadata.name,
                    label_add.stderr
                );

                let comment = run_br(
                    &workspace.workspace,
                    ["comments", "add", &issue_id, "Replay note", "--json"],
                    &surface_label(&fixture.metadata.name, "core_comment"),
                );
                assert!(
                    comment.status.success(),
                    "{} comments add failed: {}",
                    fixture.metadata.name,
                    comment.stderr
                );

                let close = run_br(
                    &workspace.workspace,
                    ["close", &issue_id, "--reason", "Replay close", "--json"],
                    &surface_label(&fixture.metadata.name, "core_close"),
                );
                assert!(
                    close.status.success(),
                    "{} close failed: {}",
                    fixture.metadata.name,
                    close.stderr
                );

                let reopen = run_br(
                    &workspace.workspace,
                    ["reopen", &issue_id, "--json"],
                    &surface_label(&fixture.metadata.name, "core_reopen"),
                );
                assert!(
                    reopen.status.success(),
                    "{} reopen failed: {}",
                    fixture.metadata.name,
                    reopen.stderr
                );

                let delete = run_br(
                    &workspace.workspace,
                    ["delete", &issue_id, "--json"],
                    &surface_label(&fixture.metadata.name, "core_delete"),
                );
                assert!(
                    delete.status.success(),
                    "{} delete failed: {}",
                    fixture.metadata.name,
                    delete.stderr
                );
            }
            WorkspaceFailureCommandOutcome::FailsPrefixMismatch => {
                assert_config_error(
                    &create,
                    "Prefix mismatch",
                    &format!("{} core create", fixture.metadata.name),
                );
            }
            WorkspaceFailureCommandOutcome::FailsConflictMarkers => {
                assert_config_error(
                    &create,
                    "Merge conflict markers detected",
                    &format!("{} core create", fixture.metadata.name),
                );
            }
            other => panic!(
                "{} has unsupported create outcome for core write replay: {:?}",
                fixture.metadata.name, other
            ),
        }
    }
}
