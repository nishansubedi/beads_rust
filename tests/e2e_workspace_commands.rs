//! E2E tests for workspace initialization and diagnostic commands.
//!
//! Tests init, config, doctor, info, where, and version commands.
//! Part of beads_rust-6esx.

mod common;

use common::cli::{BrWorkspace, extract_json_payload, run_br, run_br_with_env};
use fsqlite::Connection;
use serde_json::Value;
use std::fs;

// ============================================================================
// init command tests
// ============================================================================

#[test]
fn e2e_init_new_workspace() {
    let _log = common::test_log("e2e_init_new_workspace");
    let workspace = BrWorkspace::new();

    // Initialize a new workspace
    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);
    assert!(
        init.stdout.contains("Initialized") || init.stdout.contains("initialized"),
        "init should report success: {}",
        init.stdout
    );

    // Verify .beads directory was created
    let beads_dir = workspace.root.join(".beads");
    assert!(beads_dir.exists(), ".beads directory should exist");

    // Verify database file exists
    let db_path = beads_dir.join("beads.db");
    assert!(db_path.exists(), "beads.db should exist");
}

#[test]
fn e2e_init_already_initialized() {
    let _log = common::test_log("e2e_init_already_initialized");
    let workspace = BrWorkspace::new();

    // First init
    let init1 = run_br(&workspace, ["init"], "init1");
    assert!(
        init1.status.success(),
        "first init failed: {}",
        init1.stderr
    );

    // Second init without --force should warn or succeed gracefully
    let init2 = run_br(&workspace, ["init"], "init2");
    // Either succeeds with warning or fails gracefully with "already" message
    // br returns JSON error with code "ALREADY_INITIALIZED"
    let stderr_lower = init2.stderr.to_lowercase();
    assert!(
        init2.status.success()
            || stderr_lower.contains("already")
            || init2.stderr.contains("ALREADY_INITIALIZED"),
        "second init should succeed or warn: stdout='{}', stderr='{}'",
        init2.stdout,
        init2.stderr
    );
}

#[test]
fn e2e_init_force_reinit() {
    let _log = common::test_log("e2e_init_force_reinit");
    let workspace = BrWorkspace::new();

    // First init
    let init1 = run_br(&workspace, ["init"], "init1");
    assert!(
        init1.status.success(),
        "first init failed: {}",
        init1.stderr
    );

    // Create an issue to verify database is reset
    let create = run_br(&workspace, ["create", "Test issue before force"], "create");
    assert!(create.status.success(), "create failed: {}", create.stderr);

    // Force reinit (if supported)
    let init2 = run_br(&workspace, ["init", "--force"], "init2_force");
    // --force may not be implemented, check either way
    if init2.status.success() {
        // After force reinit, the database should be fresh
        // List should show no issues or only one if --force doesn't clear
        let list = run_br(&workspace, ["list", "--json"], "list_after_force");
        assert!(
            list.status.success(),
            "list after force init failed: {}",
            list.stderr
        );
    }
}

#[test]
fn e2e_init_creates_jsonl() {
    let _log = common::test_log("e2e_init_creates_jsonl");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Create an issue and sync to JSONL
    let create = run_br(&workspace, ["create", "JSONL test issue"], "create");
    assert!(create.status.success(), "create failed: {}", create.stderr);

    let sync = run_br(&workspace, ["sync", "--flush-only"], "sync");
    assert!(sync.status.success(), "sync failed: {}", sync.stderr);

    // Verify JSONL file exists
    let jsonl_path = workspace.root.join(".beads").join("issues.jsonl");
    assert!(jsonl_path.exists(), "issues.jsonl should exist after sync");

    let contents = fs::read_to_string(&jsonl_path).expect("read jsonl");
    assert!(
        contents.contains("JSONL test issue"),
        "JSONL should contain the issue"
    );
}

// ============================================================================
// config command tests
// ============================================================================

#[test]
fn e2e_config_list() {
    let _log = common::test_log("e2e_config_list");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // List config
    let config_list = run_br(&workspace, ["config", "list"], "config_list");
    assert!(
        config_list.status.success(),
        "config list failed: {}",
        config_list.stderr
    );
    // Should output something (even if empty)
}

#[test]
fn e2e_config_get_set() {
    let _log = common::test_log("e2e_config_get_set");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Use a unique test key that won't conflict with defaults
    // Note: issue_prefix may have DB defaults that take precedence over YAML
    let set = run_br(
        &workspace,
        ["config", "set", "test_custom_key=TESTVALUE"],
        "config_set",
    );
    assert!(set.status.success(), "config set failed: {}", set.stderr);

    // Get the config value
    let get = run_br(
        &workspace,
        ["config", "get", "test_custom_key"],
        "config_get",
    );
    assert!(get.status.success(), "config get failed: {}", get.stderr);
    assert!(
        get.stdout.contains("TESTVALUE"),
        "config get should return TESTVALUE: {}",
        get.stdout
    );
}

#[test]
fn e2e_config_json_output() {
    let _log = common::test_log("e2e_config_json_output");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // List config with --json
    let config_list = run_br(&workspace, ["config", "list", "--json"], "config_list_json");
    assert!(
        config_list.status.success(),
        "config list --json failed: {}",
        config_list.stderr
    );

    // Should be valid JSON
    let payload = extract_json_payload(&config_list.stdout);
    let _json: Value =
        serde_json::from_str(&payload).expect("config list should output valid JSON");
}

#[test]
fn e2e_update_quiet_suppresses_success_output() {
    let _log = common::test_log("e2e_update_quiet_suppresses_success_output");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    let create = run_br(
        &workspace,
        ["create", "Quiet update test", "--json"],
        "create_quiet_update",
    );
    assert!(create.status.success(), "create failed: {}", create.stderr);
    let payload = extract_json_payload(&create.stdout);
    let issue: Value = serde_json::from_str(&payload).expect("parse create json");
    let id = issue["id"].as_str().expect("issue id");

    let update = run_br(
        &workspace,
        ["--quiet", "update", id, "--status", "in_progress"],
        "update_quiet",
    );
    assert!(update.status.success(), "update failed: {}", update.stderr);
    assert!(
        update.stdout.trim().is_empty(),
        "quiet update should suppress success output: {}",
        update.stdout
    );
}

#[cfg(not(windows))]
#[test]
fn e2e_config_edit_creates_user_config() {
    let _log = common::test_log("e2e_config_edit_creates_user_config");
    let workspace = BrWorkspace::new();

    let env_vars = vec![("EDITOR", "true")];
    let edit = run_br_with_env(&workspace, ["config", "edit"], env_vars, "config_edit");
    assert!(edit.status.success(), "config edit failed: {}", edit.stderr);

    let config_path = workspace
        .root
        .join(".config")
        .join("beads")
        .join("config.yaml");
    assert!(
        config_path.exists(),
        "config edit should create user config at {}",
        config_path.display()
    );

    let contents = fs::read_to_string(&config_path).expect("read user config");
    assert!(
        contents.contains("br configuration"),
        "config edit should create default template content"
    );
}

// ============================================================================
// doctor command tests
// ============================================================================

#[test]
fn e2e_doctor_healthy_workspace() {
    let _log = common::test_log("e2e_doctor_healthy_workspace");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Run doctor on healthy workspace
    let doctor = run_br(&workspace, ["doctor"], "doctor");
    assert!(
        doctor.status.success(),
        "doctor failed on healthy workspace: {}",
        doctor.stderr
    );
}

#[test]
fn e2e_doctor_uninitialized() {
    let _log = common::test_log("e2e_doctor_uninitialized");
    let workspace = BrWorkspace::new();

    // Run doctor without init
    let doctor = run_br(&workspace, ["doctor"], "doctor_no_init");
    // Should fail or warn about missing workspace
    assert!(
        !doctor.status.success()
            || doctor.stderr.contains("not found")
            || doctor.stderr.contains("not initialized")
            || doctor.stdout.contains("not found")
            || doctor.stdout.contains("not initialized"),
        "doctor should report missing workspace: stdout='{}', stderr='{}'",
        doctor.stdout,
        doctor.stderr
    );
}

#[test]
fn e2e_doctor_json_output() {
    let _log = common::test_log("e2e_doctor_json_output");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Doctor with --json
    let doctor = run_br(&workspace, ["doctor", "--json"], "doctor_json");
    assert!(
        doctor.status.success(),
        "doctor --json failed: {}",
        doctor.stderr
    );

    let payload = extract_json_payload(&doctor.stdout);
    let _json: Value = serde_json::from_str(&payload).expect("doctor should output valid JSON");
}

#[test]
fn e2e_doctor_detects_issues() {
    let _log = common::test_log("e2e_doctor_detects_issues");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Create some issues with potential problems
    let create1 = run_br(&workspace, ["create", "Issue with missing dep"], "create1");
    assert!(create1.status.success());

    // Extract the issue ID
    let id = create1
        .stdout
        .lines()
        .next()
        .unwrap_or("")
        .strip_prefix("Created ")
        .and_then(|s| s.split(':').next())
        .unwrap_or("")
        .trim();

    // Try to add a non-existent dependency (should fail)
    let _dep = run_br(
        &workspace,
        ["dep", "add", id, "nonexistent-id"],
        "add_bad_dep",
    );
    // This may fail, which is expected

    // Run doctor
    let doctor = run_br(&workspace, ["doctor"], "doctor_check");
    assert!(doctor.status.success(), "doctor failed: {}", doctor.stderr);
}

#[test]
fn e2e_doctor_repair_json_rebuilds_and_returns_single_payload() {
    let _log = common::test_log("e2e_doctor_repair_json_rebuilds_and_returns_single_payload");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    let create = run_br(&workspace, ["create", "Repair doctor JSON"], "create");
    assert!(create.status.success(), "create failed: {}", create.stderr);

    let db_path = workspace.root.join(".beads").join("beads.db");
    let jsonl_path = workspace.root.join(".beads").join("issues.jsonl");
    assert!(db_path.exists(), "database should exist before repair test");
    assert!(
        jsonl_path.exists(),
        "issues.jsonl should exist before repair test"
    );

    let conn = Connection::open(db_path.to_string_lossy().into_owned()).expect("open beads db");
    conn.execute("INSERT INTO config (key, value) VALUES ('issue_prefix', 'dup-a')")
        .expect("insert duplicate config row a");
    conn.execute("INSERT INTO config (key, value) VALUES ('issue_prefix', 'dup-b')")
        .expect("insert duplicate config row b");

    let pre_repair = run_br(&workspace, ["doctor", "--json"], "doctor_pre_repair_json");
    assert!(
        !pre_repair.status.success(),
        "doctor should fail before repair when recoverable anomalies are present"
    );
    let pre_payload = extract_json_payload(&pre_repair.stdout);
    let pre_json: Value = serde_json::from_str(&pre_payload).expect("pre-repair doctor json");
    assert_eq!(pre_json["ok"], Value::Bool(false));

    let repaired = run_br(
        &workspace,
        ["doctor", "--repair", "--json"],
        "doctor_repair_json",
    );
    assert!(
        repaired.status.success(),
        "doctor --repair --json failed: stdout='{}' stderr='{}'",
        repaired.stdout,
        repaired.stderr
    );

    let payload = extract_json_payload(&repaired.stdout);
    let json: Value = serde_json::from_str(&payload).expect("repair doctor json");
    assert_eq!(json["repaired"], Value::Bool(true));
    assert_eq!(json["verified"], Value::Bool(true));
    assert_eq!(json["report"]["ok"], Value::Bool(false));
    assert_eq!(json["post_repair"]["ok"], Value::Bool(true));

    let anomaly_checks = json["report"]["checks"]
        .as_array()
        .expect("initial checks array");
    assert!(
        anomaly_checks.iter().any(|check| {
            check["name"] == "db.recoverable_anomalies" && check["status"] == "error"
        }),
        "expected recoverable anomaly in initial doctor report: {json:?}"
    );
}

#[test]
fn e2e_doctor_repair_json_rebuilds_when_db_is_missing() {
    let _log = common::test_log("e2e_doctor_repair_json_rebuilds_when_db_is_missing");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    let create = run_br(&workspace, ["create", "Repair doctor missing DB"], "create");
    assert!(create.status.success(), "create failed: {}", create.stderr);

    let db_path = workspace.root.join(".beads").join("beads.db");
    let jsonl_path = workspace.root.join(".beads").join("issues.jsonl");
    assert!(db_path.exists(), "database should exist before deletion");
    assert!(
        jsonl_path.exists(),
        "issues.jsonl should exist before repair test"
    );

    fs::remove_file(&db_path).expect("remove beads db");
    assert!(
        !db_path.exists(),
        "database should be missing before repair"
    );

    let repaired = run_br(
        &workspace,
        ["doctor", "--repair", "--json"],
        "doctor_repair_missing_db_json",
    );
    assert!(
        repaired.status.success(),
        "doctor --repair --json failed for missing db: stdout='{}' stderr='{}'",
        repaired.stdout,
        repaired.stderr
    );

    let payload = extract_json_payload(&repaired.stdout);
    let json: Value = serde_json::from_str(&payload).expect("repair doctor json");
    assert_eq!(json["repaired"], Value::Bool(true));
    assert_eq!(json["verified"], Value::Bool(true));
    assert_eq!(json["report"]["ok"], Value::Bool(false));
    assert_eq!(json["post_repair"]["ok"], Value::Bool(true));
    assert!(
        db_path.exists(),
        "doctor repair should recreate the database from JSONL"
    );
}

#[test]
fn e2e_doctor_repair_json_rebuilds_when_db_is_malformed() {
    let _log = common::test_log("e2e_doctor_repair_json_rebuilds_when_db_is_malformed");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    let create = run_br(
        &workspace,
        ["create", "Repair doctor malformed DB"],
        "create",
    );
    assert!(create.status.success(), "create failed: {}", create.stderr);

    let db_path = workspace.root.join(".beads").join("beads.db");
    let jsonl_path = workspace.root.join(".beads").join("issues.jsonl");
    assert!(db_path.exists(), "database should exist before corruption");
    assert!(
        jsonl_path.exists(),
        "issues.jsonl should exist before malformed-db repair test"
    );

    fs::write(&db_path, b"not a sqlite database").expect("corrupt beads db");

    let repaired = run_br(
        &workspace,
        ["doctor", "--repair", "--json"],
        "doctor_repair_malformed_db_json",
    );
    assert!(
        repaired.status.success(),
        "doctor --repair --json failed for malformed db: stdout='{}' stderr='{}'",
        repaired.stdout,
        repaired.stderr
    );

    let payload = extract_json_payload(&repaired.stdout);
    let json: Value = serde_json::from_str(&payload).expect("repair doctor json");
    assert_eq!(json["repaired"], Value::Bool(true));
    assert_eq!(json["verified"], Value::Bool(true));
    assert_eq!(json["report"]["ok"], Value::Bool(false));
    assert_eq!(json["post_repair"]["ok"], Value::Bool(true));

    let show = run_br(
        &workspace,
        ["list", "--json"],
        "list_after_malformed_repair",
    );
    assert!(
        show.status.success(),
        "list should succeed after malformed-db repair: {}",
        show.stderr
    );
    let listed_payload = extract_json_payload(&show.stdout);
    let listed: Value = serde_json::from_str(&listed_payload).expect("list json");
    assert!(
        listed.as_array().is_some_and(|issues| !issues.is_empty()),
        "expected repaired database to contain at least one issue: {listed}"
    );
}

#[test]
fn e2e_doctor_detects_and_quarantines_anomalous_wal_sidecar() {
    let _log = common::test_log("e2e_doctor_detects_and_quarantines_anomalous_wal_sidecar");
    let seed_sidecar_anomaly =
        |workspace: &BrWorkspace, label_prefix: &str| -> std::path::PathBuf {
            let init = run_br(workspace, ["init"], &format!("{label_prefix}_init"));
            assert!(init.status.success(), "init failed: {}", init.stderr);

            let create = run_br(
                workspace,
                ["create", "Repair doctor anomalous sidecar"],
                &format!("{label_prefix}_create"),
            );
            assert!(create.status.success(), "create failed: {}", create.stderr);

            let beads_dir = workspace.root.join(".beads");
            let wal_path = beads_dir.join("beads.db-wal");
            fs::write(&wal_path, b"synthetic orphan wal").expect("seed anomalous wal");
            assert!(
                !beads_dir.join("beads.db-shm").exists(),
                "fixture should keep the WAL anomaly isolated to a missing SHM sidecar"
            );
            wal_path
        };

    let detect_workspace = BrWorkspace::new();
    let _detect_wal_path = seed_sidecar_anomaly(&detect_workspace, "detect");

    let doctor = run_br(
        &detect_workspace,
        ["doctor", "--json"],
        "doctor_sidecar_json",
    );
    assert!(
        !doctor.status.success(),
        "doctor should fail when anomalous sidecars are present"
    );
    let doctor_json: Value =
        serde_json::from_str(&extract_json_payload(&doctor.stdout)).expect("doctor json");
    assert_eq!(doctor_json["ok"], Value::Bool(false));
    assert!(
        doctor_json["checks"]
            .as_array()
            .is_some_and(|checks| checks.iter().any(|check| {
                check["name"] == "db.sidecars"
                    && check["status"] == "error"
                    && check["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("matching SHM sidecar"))
            })),
        "doctor should surface the sidecar anomaly: {doctor_json}"
    );

    let repair_workspace = BrWorkspace::new();
    let wal_path = seed_sidecar_anomaly(&repair_workspace, "repair");
    let repair_beads_dir = repair_workspace.root.join(".beads");

    let repaired = run_br(
        &repair_workspace,
        ["doctor", "--repair", "--json"],
        "doctor_repair_sidecar_json",
    );
    assert!(
        repaired.status.success(),
        "doctor --repair --json failed for anomalous sidecar: stdout='{}' stderr='{}'",
        repaired.stdout,
        repaired.stderr
    );

    let repaired_json: Value =
        serde_json::from_str(&extract_json_payload(&repaired.stdout)).expect("repair doctor json");
    assert_eq!(repaired_json["repaired"], Value::Bool(true));
    assert_eq!(repaired_json["verified"], Value::Bool(true));
    assert_eq!(repaired_json["post_repair"]["ok"], Value::Bool(true));
    assert!(
        repaired_json["local_repair"]["quarantined_artifacts"]
            .as_array()
            .is_some_and(|artifacts| !artifacts.is_empty()),
        "repair should preserve anomalous sidecars in recovery: {repaired_json}"
    );
    assert!(
        !wal_path.exists(),
        "anomalous WAL should be moved out of the live database family"
    );

    let recovery_dir = repair_beads_dir.join(".br_recovery");
    let recovery_entries: Vec<_> = fs::read_dir(&recovery_dir)
        .expect("read recovery dir")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        recovery_entries
            .iter()
            .any(|name| name.starts_with("beads.db-wal.") && name.ends_with(".doctor-quarantine")),
        "expected quarantined WAL artifact in recovery dir: {recovery_entries:?}"
    );
}

// ============================================================================
// info command tests
// ============================================================================

#[test]
fn e2e_info_basic() {
    let _log = common::test_log("e2e_info_basic");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Run info command
    let info = run_br(&workspace, ["info"], "info");
    assert!(info.status.success(), "info failed: {}", info.stderr);

    // Should contain path information
    assert!(
        info.stdout.contains(".beads") || info.stdout.contains("beads"),
        "info should mention beads directory: {}",
        info.stdout
    );
}

#[test]
fn e2e_info_json_output() {
    let _log = common::test_log("e2e_info_json_output");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Info with --json
    let info = run_br(&workspace, ["info", "--json"], "info_json");
    assert!(info.status.success(), "info --json failed: {}", info.stderr);

    let payload = extract_json_payload(&info.stdout);
    let json: Value = serde_json::from_str(&payload).expect("info should output valid JSON");

    // Should have workspace path (br uses "database_path")
    assert!(
        json.get("workspace_path").is_some()
            || json.get("db_path").is_some()
            || json.get("path").is_some()
            || json.get("database_path").is_some(),
        "info JSON should contain path info: {json}"
    );
}

#[test]
fn e2e_info_uninitialized() {
    let _log = common::test_log("e2e_info_uninitialized");
    let workspace = BrWorkspace::new();

    // Run info without init
    let info = run_br(&workspace, ["info"], "info_no_init");
    // Should fail or report no workspace
    assert!(
        !info.status.success()
            || info.stderr.contains("not found")
            || info.stdout.contains("not found"),
        "info should report missing workspace"
    );
}

// ============================================================================
// where command tests
// ============================================================================

#[test]
fn e2e_where_basic() {
    let _log = common::test_log("e2e_where_basic");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Run where command
    let whr = run_br(&workspace, ["where"], "where");
    assert!(whr.status.success(), "where failed: {}", whr.stderr);

    // Should output the .beads path
    assert!(
        whr.stdout.contains(".beads"),
        "where should output .beads path: {}",
        whr.stdout
    );
    assert!(
        whr.stdout.contains("database:"),
        "where should report the resolved database path: {}",
        whr.stdout
    );
    assert!(
        whr.stdout.contains("jsonl:"),
        "where should report the resolved JSONL path: {}",
        whr.stdout
    );
}

#[test]
fn e2e_where_uninitialized() {
    let _log = common::test_log("e2e_where_uninitialized");
    let workspace = BrWorkspace::new();

    // Run where without init
    let whr = run_br(&workspace, ["where"], "where_no_init");
    assert!(!whr.status.success(), "where should fail without init");

    let error_payload = extract_json_payload(&whr.stderr);
    let error_json: Value = serde_json::from_str(&error_payload)
        .expect("where without init should emit structured json to stderr");
    assert_eq!(error_json["error"]["code"], "NOT_INITIALIZED");
    assert!(
        error_json["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("br init")),
        "structured error should tell the user how to initialize the workspace"
    );
}

#[test]
fn e2e_where_json_output() {
    let _log = common::test_log("e2e_where_json_output");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    // Where with explicit JSON output
    let whr = run_br(&workspace, ["where", "--json"], "where_json");
    assert!(whr.status.success(), "where --json failed: {}", whr.stderr);
    let payload = extract_json_payload(&whr.stdout);
    let _json: Value =
        serde_json::from_str(&payload).expect("where --json should output valid JSON");
}

#[test]
fn e2e_where_json_reports_effective_prefix_from_project_config() {
    let _log = common::test_log("e2e_where_json_reports_effective_prefix_from_project_config");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    fs::write(
        workspace.root.join(".beads").join("config.yaml"),
        "issue_prefix: proj\n",
    )
    .expect("write project config");

    let whr = run_br(&workspace, ["where", "--json"], "where_json_config_prefix");
    assert!(whr.status.success(), "where --json failed: {}", whr.stderr);

    let payload = extract_json_payload(&whr.stdout);
    let json: Value =
        serde_json::from_str(&payload).expect("where --json should output valid JSON");
    assert_eq!(json["prefix"].as_str(), Some("proj"));
}

#[test]
fn e2e_where_json_omits_prefix_for_mixed_jsonl_fallback() {
    let _log = common::test_log("e2e_where_json_omits_prefix_for_mixed_jsonl_fallback");
    let workspace = BrWorkspace::new();
    let beads_dir = workspace.root.join(".beads");
    fs::create_dir_all(&beads_dir).expect("create beads dir");
    fs::write(
        beads_dir.join("issues.jsonl"),
        concat!(
            r#"{"id":"proj-abc12","title":"Example"}"#,
            "\n",
            r#"{"id":"other-def34","title":"Second"}"#,
            "\n",
        ),
    )
    .expect("write mixed-prefix jsonl");

    let whr = run_br(
        &workspace,
        ["where", "--json"],
        "where_json_mixed_prefix_jsonl",
    );
    assert!(whr.status.success(), "where --json failed: {}", whr.stderr);

    let payload = extract_json_payload(&whr.stdout);
    let json: Value =
        serde_json::from_str(&payload).expect("where --json should output valid JSON");
    assert!(
        json.get("prefix").is_none(),
        "where should omit misleading prefix when JSONL prefixes conflict: {json}"
    );
}

#[test]
fn e2e_where_json_recovers_prefix_from_valid_lines_despite_malformed_jsonl_entries() {
    let _log = common::test_log(
        "e2e_where_json_recovers_prefix_from_valid_lines_despite_malformed_jsonl_entries",
    );
    let workspace = BrWorkspace::new();
    let beads_dir = workspace.root.join(".beads");
    fs::create_dir_all(&beads_dir).expect("create beads dir");
    fs::write(
        beads_dir.join("issues.jsonl"),
        concat!(
            "{not valid json}\n",
            r#"{"id":"proj-abc12","title":"Example"}"#,
            "\n",
        ),
    )
    .expect("write malformed jsonl");

    let whr = run_br(
        &workspace,
        ["where", "--json"],
        "where_json_malformed_prefix_jsonl",
    );
    assert!(whr.status.success(), "where --json failed: {}", whr.stderr);

    let payload = extract_json_payload(&whr.stdout);
    let json: Value =
        serde_json::from_str(&payload).expect("where --json should output valid JSON");
    assert_eq!(json["prefix"].as_str(), Some("proj"));
}

#[test]
fn e2e_where_json_omits_prefix_for_mixed_jsonl_even_with_existing_db_prefix() {
    let _log = common::test_log(
        "e2e_where_json_omits_prefix_for_mixed_jsonl_even_with_existing_db_prefix",
    );
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success(), "init failed: {}", init.stderr);

    fs::write(
        workspace.root.join(".beads").join("issues.jsonl"),
        concat!(
            r#"{"id":"proj-abc12","title":"Example"}"#,
            "\n",
            r#"{"id":"other-def34","title":"Second"}"#,
            "\n",
        ),
    )
    .expect("write mixed-prefix jsonl");

    let whr = run_br(
        &workspace,
        ["where", "--json"],
        "where_json_mixed_prefix_existing_db",
    );
    assert!(whr.status.success(), "where --json failed: {}", whr.stderr);

    let payload = extract_json_payload(&whr.stdout);
    let json: Value =
        serde_json::from_str(&payload).expect("where --json should output valid JSON");
    assert!(
        json.get("prefix").is_none(),
        "where should omit misleading prefix when JSONL prefixes conflict even if a DB exists: {json}"
    );
}

// ============================================================================
// version command tests
// ============================================================================

#[test]
fn e2e_version_basic() {
    let _log = common::test_log("e2e_version_basic");
    let workspace = BrWorkspace::new();

    // Version doesn't require init
    let version = run_br(&workspace, ["version"], "version");
    assert!(
        version.status.success(),
        "version failed: {}",
        version.stderr
    );

    // Should contain version number
    assert!(
        version.stdout.contains("0.") || version.stdout.contains("1."),
        "version should contain version number: {}",
        version.stdout
    );
}

#[test]
fn e2e_version_json_output() {
    let _log = common::test_log("e2e_version_json_output");
    let workspace = BrWorkspace::new();

    // Version with --json
    let version = run_br(&workspace, ["version", "--json"], "version_json");
    assert!(
        version.status.success(),
        "version --json failed: {}",
        version.stderr
    );

    let payload = extract_json_payload(&version.stdout);
    let json: Value = serde_json::from_str(&payload).expect("version should output valid JSON");

    // Should have version field
    assert!(
        json.get("version").is_some() || json.get("semver").is_some(),
        "version JSON should contain version field: {json}"
    );
}

#[test]
fn e2e_version_short_flag() {
    let _log = common::test_log("e2e_version_short_flag");
    let workspace = BrWorkspace::new();

    // Test -V flag
    let version = run_br(&workspace, ["-V"], "version_short");
    assert!(version.status.success(), "-V failed: {}", version.stderr);

    assert!(
        version.stdout.contains("br")
            || version.stdout.contains("0.")
            || version.stdout.contains("1."),
        "-V should output version: {}",
        version.stdout
    );
}

#[test]
fn e2e_version_help() {
    let _log = common::test_log("e2e_version_help");
    let workspace = BrWorkspace::new();

    // Test --version flag
    let version = run_br(&workspace, ["--version"], "version_long");
    assert!(
        version.status.success(),
        "--version failed: {}",
        version.stderr
    );

    assert!(
        version.stdout.contains("br")
            || version.stdout.contains("0.")
            || version.stdout.contains("1."),
        "--version should output version: {}",
        version.stdout
    );
}

// ============================================================================
// Combined/integration tests
// ============================================================================

#[test]
fn e2e_full_workspace_lifecycle() {
    let _log = common::test_log("e2e_full_workspace_lifecycle");
    let workspace = BrWorkspace::new();

    // 1. Check version works without init
    let version = run_br(&workspace, ["version"], "version");
    assert!(version.status.success());

    // 2. Where should fail without init
    let where_before = run_br(&workspace, ["where"], "where_before");
    assert!(
        !where_before.status.success() || where_before.stdout.trim().is_empty(),
        "where should fail before init"
    );

    // 3. Initialize
    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success());

    // 4. Where should work now
    let where_after = run_br(&workspace, ["where"], "where_after");
    assert!(where_after.status.success());
    assert!(where_after.stdout.contains(".beads"));

    // 5. Info should show workspace details
    let info = run_br(&workspace, ["info"], "info");
    assert!(info.status.success());

    // 6. Doctor should pass
    let doctor = run_br(&workspace, ["doctor"], "doctor");
    assert!(doctor.status.success());

    // 7. Config should be accessible
    let config = run_br(&workspace, ["config", "list"], "config");
    assert!(config.status.success());
}

#[test]
fn e2e_workspace_paths_consistent() {
    let _log = common::test_log("e2e_workspace_paths_consistent");
    let workspace = BrWorkspace::new();

    let init = run_br(&workspace, ["init"], "init");
    assert!(init.status.success());

    // Get path from where
    let whr = run_br(&workspace, ["where"], "where");
    assert!(whr.status.success());
    let where_path = whr.stdout.trim();

    // Get path from info --json
    let info = run_br(&workspace, ["info", "--json"], "info_json");
    assert!(info.status.success());

    let payload = extract_json_payload(&info.stdout);
    let json: Value = serde_json::from_str(&payload).expect("valid JSON");

    // The paths should be consistent (both point to same .beads)
    if let Some(info_path) = json
        .get("workspace_path")
        .or_else(|| json.get("beads_dir"))
        .or_else(|| json.get("path"))
    {
        let info_path_str = info_path.as_str().unwrap_or("");
        // Both should contain .beads
        assert!(
            where_path.contains(".beads")
                && (info_path_str.contains(".beads") || info_path_str.is_empty()),
            "Paths should be consistent: where='{where_path}', info='{info_path_str}'"
        );
    }
}
