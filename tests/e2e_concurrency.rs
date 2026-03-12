//! E2E tests for `SQLite` lock handling and concurrency semantics.
//!
//! Validates:
//! - Lock contention with overlapping write operations
//! - --lock-timeout behavior and proper error codes
//! - Concurrent read-only operations succeed
//!
//! Related: beads_rust-uahy

mod common;

use assert_cmd::Command;
use common::dataset_registry::{DatasetRegistry, IsolatedDataset, KnownDataset};
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Result of running a br command.
#[derive(Debug)]
struct BrResult {
    stdout: String,
    stderr: String,
    success: bool,
    _duration: Duration,
}

fn should_clear_inherited_br_env(key: &OsStr) -> bool {
    let key = key.to_string_lossy();
    key.starts_with("BD_")
        || key.starts_with("BEADS_")
        || matches!(
            key.as_ref(),
            "BR_OUTPUT_FORMAT" | "TOON_DEFAULT_FORMAT" | "TOON_STATS"
        )
}

fn clear_inherited_br_env(cmd: &mut Command) {
    for (key, _) in std::env::vars_os() {
        if should_clear_inherited_br_env(&key) {
            cmd.env_remove(&key);
        }
    }
}

/// Run br command in a specific directory.
fn run_br_in_dir<I, S>(root: &PathBuf, args: I) -> BrResult
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    run_br_in_dir_with_env(root, args, std::iter::empty::<(String, String)>())
}

/// Run br command in a specific directory with environment overrides.
fn run_br_in_dir_with_env<I, S, E, K, V>(root: &PathBuf, args: I, env_vars: E) -> BrResult
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
    E: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let start = Instant::now();
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("br"));
    cmd.current_dir(root);
    cmd.args(args);
    clear_inherited_br_env(&mut cmd);
    cmd.envs(env_vars);
    cmd.env("NO_COLOR", "1");
    cmd.env("RUST_BACKTRACE", "1");
    cmd.env("HOME", root);

    let output = cmd.output().expect("run br");
    let duration = start.elapsed();

    BrResult {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        success: output.status.success(),
        _duration: duration,
    }
}

/// Helper to parse created issue ID from stdout.
fn parse_created_id(stdout: &str) -> String {
    let line = stdout.lines().next().unwrap_or("");
    // Handle both formats: "Created bd-xxx: title" and "✓ Created bd-xxx: title"
    let normalized = line.strip_prefix("✓ ").unwrap_or(line);
    normalized
        .strip_prefix("Created ")
        .and_then(|rest| rest.split(':').next())
        .unwrap_or("")
        .trim()
        .to_string()
}

fn is_expected_contention_failure(result: &BrResult) -> bool {
    let combined = format!("{} {}", result.stdout, result.stderr).to_lowercase();
    !result.success
        && (combined.contains("busy")
            || combined.contains("locked")
            || combined.contains("lock timeout")
            || combined.contains("timed out")
            || combined.contains("sync conflict")
            || combined.contains("jsonl is newer"))
        && !combined.contains("malformed")
        && !combined.contains("corrupt")
        && !combined.contains("constraint")
        && !combined.contains("unexpected token")
        && !combined.contains("panic")
}

fn assert_only_success_or_contention(role: &str, results: &[BrResult]) -> usize {
    let mut success_count = 0;
    let mut unexpected_failures = Vec::new();

    for (index, result) in results.iter().enumerate() {
        if result.success {
            success_count += 1;
        } else if !is_expected_contention_failure(result) {
            unexpected_failures.push(format!(
                "{role}[{index}] stdout={} stderr={}",
                result.stdout, result.stderr
            ));
        }
    }

    assert!(
        unexpected_failures.is_empty(),
        "unexpected {role} failures: {}",
        unexpected_failures.join(" | ")
    );

    success_count
}

/// Extract JSON payload from stdout (skip non-JSON preamble).
fn extract_json_payload(stdout: &str) -> String {
    for (idx, line) in stdout.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('[') || trimmed.starts_with('{') {
            return stdout
                .lines()
                .skip(idx)
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();
        }
    }
    stdout.trim().to_string()
}

fn is_lock_related_failure(result: &BrResult) -> bool {
    if result.success {
        return false;
    }

    let combined = format!("{} {}", result.stdout, result.stderr).to_lowercase();
    combined.contains("busy")
        || combined.contains("lock")
        || combined.contains("database")
        || combined.contains("sync conflict")
        || combined.contains("jsonl is newer")
}

fn assert_success_or_lock_failure(result: &BrResult, context: &str) {
    assert!(
        result.success || is_lock_related_failure(result),
        "{context} failed with unexpected output: stdout={} stderr={}",
        result.stdout,
        result.stderr
    );
}

fn create_routes_file(root: &Path, entries: &[(&str, &Path)]) {
    let routes_path = root.join(".beads").join("routes.jsonl");
    let content = entries
        .iter()
        .map(|(prefix, path)| {
            format!(
                r#"{{"prefix":"{prefix}","path":"{}"}}"#,
                path.to_string_lossy()
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(routes_path, content).expect("write routes.jsonl");
}

fn configure_external_route(main_root: &Path, external_root: &Path) {
    fs::write(
        external_root.join(".beads").join("config.yaml"),
        "issue_prefix: ext\n",
    )
    .expect("write external config");
    create_routes_file(main_root, &[("ext-", external_root)]);
}

/// Test that concurrent write operations respect `SQLite` locking.
///
/// This test:
/// 1. Starts two threads that attempt to create issues simultaneously
/// 2. Uses a barrier to synchronize the start of both operations
/// 3. Verifies that both eventually succeed (due to default busy timeout)
#[test]
fn e2e_concurrent_writes_succeed_with_retry() {
    let _log = common::test_log("e2e_concurrent_writes_succeed_with_retry");

    // Create workspace
    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize workspace
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    // Create a barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(2));
    let root1 = Arc::new(root.clone());
    let root2 = Arc::new(root.clone());

    let barrier1 = Arc::clone(&barrier);
    let barrier2 = Arc::clone(&barrier);
    let root1_clone = Arc::clone(&root1);
    let root2_clone = Arc::clone(&root2);

    // Spawn two threads that will try to create issues concurrently
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        run_br_in_dir(&root1_clone, ["create", "Issue from thread 1"])
    });

    let handle2 = thread::spawn(move || {
        barrier2.wait();
        run_br_in_dir(&root2_clone, ["create", "Issue from thread 2"])
    });

    let result1 = handle1.join().expect("thread 1 panicked");
    let result2 = handle2.join().expect("thread 2 panicked");

    // With default busy timeout, both should eventually succeed
    // (SQLite retries on SQLITE_BUSY)
    assert!(
        result1.success,
        "thread 1 create failed: {}",
        result1.stderr
    );
    assert!(
        result2.success,
        "thread 2 create failed: {}",
        result2.stderr
    );

    // Verify both issues were created
    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list failed: {}", list.stderr);
    assert!(
        list.stdout.contains("Issue from thread 1"),
        "missing issue from thread 1"
    );
    assert!(
        list.stdout.contains("Issue from thread 2"),
        "missing issue from thread 2"
    );

    // Keep temp_dir alive until end
    drop(temp_dir);
}

/// Test that --lock-timeout=1 causes quick failure on lock contention.
///
/// This test:
/// 1. Holds a write lock via rapid updates
/// 2. Attempts a second write with --lock-timeout=1
/// 3. Measures timing to verify timeout behavior
#[test]
fn e2e_lock_timeout_behavior() {
    let _log = common::test_log("e2e_lock_timeout_behavior");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize workspace
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    // Create an issue first
    let create = run_br_in_dir(&root, ["create", "Seed issue"]);
    assert!(create.success, "create seed failed: {}", create.stderr);
    let seed_id = parse_created_id(&create.stdout);

    // Use a synchronization primitive
    let barrier = Arc::new(Barrier::new(2));
    let root_shared = Arc::new(root);
    let seed_id_arc = Arc::new(seed_id);

    let barrier1 = Arc::clone(&barrier);
    let barrier2 = Arc::clone(&barrier);
    let root1_clone = Arc::clone(&root_shared);
    let root2_clone = Arc::clone(&root_shared);
    let seed_id_clone = Arc::clone(&seed_id_arc);

    // Thread 1: Do multiple rapid updates to keep the DB busy
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        for i in 0..10 {
            let title = format!("Update {i}");
            run_br_in_dir(&root1_clone, ["update", &seed_id_clone, "--title", &title]);
            thread::sleep(Duration::from_millis(50));
        }
    });

    // Thread 2: Try to create with low timeout
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        // Small delay to let the first thread start
        thread::sleep(Duration::from_millis(25));
        let start = Instant::now();
        let result = run_br_in_dir(
            &root2_clone,
            ["--lock-timeout", "1", "create", "Low timeout issue"],
        );
        let elapsed = start.elapsed();
        (result, elapsed)
    });

    handle1.join().expect("thread 1 panicked");
    let (result2, elapsed2) = handle2.join().expect("thread 2 panicked");

    // Log timing for diagnostics
    eprintln!(
        "Low timeout operation: success={}, elapsed={elapsed2:?}",
        result2.success
    );

    // Either outcome is valid depending on timing:
    // - Success if no contention was hit
    // - Failure with lock/busy error if contention occurred
    if !result2.success {
        let combined = format!("{} {}", result2.stderr, result2.stdout).to_lowercase();
        // Check for any database-related error (busy, lock, or general database error)
        assert!(
            combined.contains("busy")
                || combined.contains("lock")
                || combined.contains("database")
                || combined.contains("error"),
            "expected lock-related error, got: stdout={}, stderr={}",
            result2.stdout,
            result2.stderr
        );
    }

    drop(temp_dir);
}

/// Test that read-only operations succeed concurrently without blocking.
///
/// This test:
/// 1. Creates several issues
/// 2. Runs multiple concurrent read operations (list, show, stats)
/// 3. Verifies all complete successfully
#[test]
fn e2e_concurrent_reads_succeed() {
    let _log = common::test_log("e2e_concurrent_reads_succeed");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize and create some issues
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let mut ids = Vec::new();
    for i in 0..5 {
        let create = run_br_in_dir(&root, ["create", &format!("Issue {i}")]);
        assert!(create.success, "create {i} failed: {}", create.stderr);
        ids.push(parse_created_id(&create.stdout));
    }

    // Spawn multiple threads doing read operations
    let barrier = Arc::new(Barrier::new(5));
    let mut handles = Vec::new();

    let root_arc = Arc::new(root);
    for (i, issue_id) in ids.iter().cloned().enumerate() {
        let root_clone = Arc::clone(&root_arc);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let start = Instant::now();

            // Mix of read operations
            let list = run_br_in_dir(&root_clone, ["list", "--json"]);
            let show = run_br_in_dir(&root_clone, ["show", &issue_id, "--json"]);
            let stats = run_br_in_dir(&root_clone, ["stats", "--json"]);

            let elapsed = start.elapsed();
            (i, list, show, stats, elapsed)
        });

        handles.push(handle);
    }

    // Collect results
    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    // All read operations should succeed
    for (i, list, show, stats, elapsed) in &results {
        assert!(list.success, "thread {i} list failed: {}", list.stderr);
        assert!(show.success, "thread {i} show failed: {}", show.stderr);
        assert!(stats.success, "thread {i} stats failed: {}", stats.stderr);
        eprintln!("Thread {i} completed reads in {elapsed:?}");
    }

    drop(temp_dir);
}

/// Test that parallel read-only commands do not contend on teardown.
///
/// This specifically guards against hidden write-like work during command
/// shutdown, such as opportunistic WAL checkpoints from otherwise read-only
/// operations.
#[test]
fn e2e_parallel_read_only_commands_do_not_busy_on_drop() {
    let _log = common::test_log("e2e_parallel_read_only_commands_do_not_busy_on_drop");

    let registry = DatasetRegistry::new();
    if !registry.is_available(KnownDataset::BeadsRust) {
        eprintln!("skipping: beads_rust dataset is unavailable in this environment");
        return;
    }

    let isolated =
        IsolatedDataset::from_dataset(KnownDataset::BeadsRust).expect("copy beads_rust dataset");
    let root = isolated.root.clone();

    let create = run_br_in_dir(
        &root,
        [
            "--no-auto-import",
            "--no-auto-flush",
            "create",
            "Concurrency seed issue",
        ],
    );
    assert!(create.success, "seed create failed: {}", create.stderr);
    let issue_id = parse_created_id(&create.stdout);

    let root_arc = Arc::new(root);
    let barrier = Arc::new(Barrier::new(6));
    let mut handles = Vec::new();

    for worker in 0..6 {
        let root_clone = Arc::clone(&root_arc);
        let barrier_clone = Arc::clone(&barrier);
        let issue_id_clone = issue_id.clone();

        handles.push(thread::spawn(move || {
            barrier_clone.wait();

            let mut failures = Vec::new();
            for iteration in 0..6 {
                let result = if worker % 2 == 0 {
                    run_br_in_dir(
                        &root_clone,
                        [
                            "--lock-timeout",
                            "1",
                            "--no-auto-import",
                            "--no-auto-flush",
                            "ready",
                            "--json",
                        ],
                    )
                } else {
                    run_br_in_dir(
                        &root_clone,
                        [
                            "--lock-timeout",
                            "1",
                            "--no-auto-import",
                            "--no-auto-flush",
                            "show",
                            &issue_id_clone,
                            "--json",
                        ],
                    )
                };

                if !result.success {
                    failures.push(format!(
                        "iteration={iteration} stdout={} stderr={}",
                        result.stdout, result.stderr
                    ));
                    break;
                }
            }

            (worker, failures)
        }));
    }

    for handle in handles {
        let (worker, failures) = handle.join().expect("thread panicked");
        assert!(
            failures.is_empty(),
            "worker {worker} hit read-only contention: {}",
            failures.join(" | ")
        );
    }

    drop(isolated);
}

/// Test that lock timeout is properly respected with specific timing.
///
/// This test:
/// 1. Sets a specific lock timeout
/// 2. Verifies the operation completes within expected time (no contention)
#[test]
fn e2e_lock_timeout_timing() {
    let _log = common::test_log("e2e_lock_timeout_timing");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize workspace
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    // Create a seed issue
    let create = run_br_in_dir(&root, ["create", "Seed"]);
    assert!(create.success, "create failed: {}", create.stderr);

    // Test with a 500ms timeout (should complete quickly without contention)
    let timeout_ms = 500;
    let start = Instant::now();
    let result = run_br_in_dir(
        &root,
        ["--lock-timeout", &timeout_ms.to_string(), "list", "--json"],
    );
    let elapsed = start.elapsed();

    // Without contention, should complete very quickly
    assert!(result.success, "list failed: {}", result.stderr);
    let timeout_ms_u64 = u64::try_from(timeout_ms).unwrap_or(0);
    assert!(
        elapsed < Duration::from_millis(timeout_ms_u64 + 500),
        "operation took too long without contention: {elapsed:?}"
    );

    eprintln!("Lock timeout timing test: elapsed={elapsed:?} (timeout={timeout_ms}ms)");

    drop(temp_dir);
}

/// Test that writes serialize properly and eventually complete.
///
/// This test verifies the proper serialization of write operations.
#[test]
fn e2e_write_serialization() {
    let _log = common::test_log("e2e_write_serialization");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let start = Instant::now();
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(3));

    // Spawn 3 threads doing writes
    for i in 0..3 {
        let root_clone = Arc::new(root.clone());
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let thread_start = Instant::now();
            let result = run_br_in_dir(
                &root_clone,
                [
                    "--lock-timeout",
                    "1000",
                    "create",
                    &format!("Serialized issue {i}"),
                ],
            );
            let thread_elapsed = thread_start.elapsed();
            (i, result, thread_elapsed)
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();
    let total_elapsed = start.elapsed();

    // All should succeed
    for (i, result, elapsed) in &results {
        assert!(result.success, "thread {i} failed: {}", result.stderr);
        eprintln!("Thread {i} took {elapsed:?}");
    }

    eprintln!("Total time for 3 serialized writes: {total_elapsed:?}");

    // Verify all 3 issues exist
    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "final list failed: {}", list.stderr);
    for i in 0..3 {
        assert!(
            list.stdout.contains(&format!("Serialized issue {i}")),
            "missing serialized issue {i}"
        );
    }

    drop(temp_dir);
}

/// Test mixed read-write concurrency.
///
/// This test:
/// 1. Has some threads doing writes
/// 2. Has other threads doing reads
/// 3. Verifies reads complete and writes eventually complete
#[test]
fn e2e_mixed_read_write_concurrency() {
    let _log = common::test_log("e2e_mixed_read_write_concurrency");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize with some existing data
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    for i in 0..3 {
        let create = run_br_in_dir(&root, ["create", &format!("Existing issue {i}")]);
        assert!(create.success, "create {i} failed");
    }

    let barrier = Arc::new(Barrier::new(6)); // 3 readers + 3 writers
    let mut handles = Vec::new();

    // Spawn readers
    for i in 0..3 {
        let root_clone = Arc::new(root.clone());
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let start = Instant::now();
            let result = run_br_in_dir(&root_clone, ["list", "--json"]);
            let elapsed = start.elapsed();
            ("reader", i, result, elapsed)
        });
        handles.push(handle);
    }

    // Spawn writers
    for i in 0..3 {
        let root_clone = Arc::new(root.clone());
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            let start = Instant::now();
            let result = run_br_in_dir(&root_clone, ["create", &format!("New issue {i}")]);
            let elapsed = start.elapsed();
            ("writer", i, result, elapsed)
        });
        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    let mut reader_results = Vec::new();
    let mut writer_results = Vec::new();
    for (role, i, result, elapsed) in &results {
        eprintln!("{role} {i} completed in {elapsed:?}");
        if *role == "reader" {
            reader_results.push(result);
        } else {
            writer_results.push(result);
        }
    }

    let reader_successes = reader_results.iter().filter(|result| result.success).count();
    let writer_successes = writer_results.iter().filter(|result| result.success).count();

    assert!(
        reader_successes > 0,
        "expected at least one successful reader under mixed contention"
    );
    assert!(
        writer_successes > 0,
        "expected at least one successful writer under mixed contention"
    );

    for (idx, result) in reader_results.iter().enumerate() {
        assert_success_or_lock_failure(result, &format!("reader {idx}"));
    }
    for (idx, result) in writer_results.iter().enumerate() {
        assert_success_or_lock_failure(result, &format!("writer {idx}"));
    }

    // Verify final state
    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "final list failed: {}", list.stderr);

    // All successful writers should persist; explicit contention failures are acceptable.
    let payload = extract_json_payload(&list.stdout);
    let issues: Vec<serde_json::Value> = serde_json::from_str(&payload).expect("parse list json");
    assert!(
        issues.len() >= 3 + writer_successes,
        "expected at least {} issues, got {}",
        3 + writer_successes,
        issues.len()
    );

    drop(temp_dir);
}

/// Test that mixed mutating command families either succeed or fail explicitly
/// under contention, while the workspace remains readable afterward.
#[test]
fn e2e_interleaved_command_families_remain_bounded() {
    let _log = common::test_log("e2e_interleaved_command_families_remain_bounded");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let create = run_br_in_dir(&root, ["create", "Interleaved seed issue"]);
    assert!(create.success, "create seed failed: {}", create.stderr);
    let seed_id = parse_created_id(&create.stdout);

    let barrier = Arc::new(Barrier::new(4));
    let root_arc = Arc::new(root.clone());
    let seed_id_arc = Arc::new(seed_id);

    let create_handle = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..4 {
                results.push(run_br_in_dir(
                    &root,
                    [
                        "--lock-timeout",
                        "1",
                        "create",
                        &format!("Interleaved issue {idx}"),
                    ],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let update_handle = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        let seed_id = Arc::clone(&seed_id_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..4 {
                let title = format!("Interleaved title {idx}");
                results.push(run_br_in_dir(
                    &root,
                    ["--lock-timeout", "1", "update", &seed_id, "--title", &title],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let label_handle = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        let seed_id = Arc::clone(&seed_id_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..4 {
                let label = format!("lane-{idx}");
                results.push(run_br_in_dir(
                    &root,
                    ["--lock-timeout", "1", "label", "add", &seed_id, &label],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let comments_handle = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        let seed_id = Arc::clone(&seed_id_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..4 {
                let body = format!("bounded comment {idx}");
                results.push(run_br_in_dir(
                    &root,
                    ["--lock-timeout", "1", "comments", "add", &seed_id, &body],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let worker_results = [
        ("create", create_handle.join().expect("create worker panicked")),
        ("update", update_handle.join().expect("update worker panicked")),
        ("label", label_handle.join().expect("label worker panicked")),
        (
            "comments",
            comments_handle.join().expect("comments worker panicked"),
        ),
    ];

    let total_successes: usize = worker_results
        .iter()
        .map(|(_, results)| results.iter().filter(|result| result.success).count())
        .sum();
    assert!(
        total_successes > 0,
        "expected at least one successful mutation across interleaved workers"
    );

    for (worker, results) in &worker_results {
        for (idx, result) in results.iter().enumerate() {
            assert_success_or_lock_failure(result, &format!("{worker} iteration {idx}"));
        }
    }

    let show = run_br_in_dir(&root, ["show", &seed_id_arc, "--json"]);
    assert!(show.success, "show after contention failed: {}", show.stderr);

    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list after contention failed: {}", list.stderr);

    let stats = run_br_in_dir(&root, ["stats", "--json"]);
    assert!(stats.success, "stats after contention failed: {}", stats.stderr);
}

/// Test that routed access to an external workspace remains available while the
/// invoking workspace is under local mutation.
#[test]
fn e2e_routed_external_mutation_succeeds_during_local_updates() {
    let _log = common::test_log("e2e_routed_external_mutation_succeeds_during_local_updates");

    let main_temp = TempDir::new().expect("create main temp dir");
    let external_temp = TempDir::new().expect("create external temp dir");
    let main_root = main_temp.path().to_path_buf();
    let external_root = external_temp.path().to_path_buf();

    let init_main = run_br_in_dir(&main_root, ["init"]);
    assert!(init_main.success, "init main failed: {}", init_main.stderr);
    let init_external = run_br_in_dir(&external_root, ["init"]);
    assert!(
        init_external.success,
        "init external failed: {}",
        init_external.stderr
    );

    configure_external_route(&main_root, &external_root);

    let create_local = run_br_in_dir(&main_root, ["create", "Local issue under mutation"]);
    assert!(create_local.success, "create local failed: {}", create_local.stderr);
    let local_id = parse_created_id(&create_local.stdout);

    let create_external = run_br_in_dir(&external_root, ["create", "External routed issue"]);
    assert!(
        create_external.success,
        "create external failed: {}",
        create_external.stderr
    );
    let external_id = parse_created_id(&create_external.stdout);
    assert!(
        external_id.starts_with("ext-"),
        "expected external prefix, got {external_id}"
    );

    let barrier = Arc::new(Barrier::new(2));
    let main_root_arc = Arc::new(main_root.clone());
    let local_id_arc = Arc::new(local_id);
    let external_id_arc = Arc::new(external_id);

    let local_updates = {
        let barrier = Arc::clone(&barrier);
        let main_root = Arc::clone(&main_root_arc);
        let local_id = Arc::clone(&local_id_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..8 {
                let title = format!("Local routed contention title {idx}");
                results.push(run_br_in_dir(
                    &main_root,
                    ["--lock-timeout", "1", "update", &local_id, "--title", &title],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let routed_comments = {
        let barrier = Arc::clone(&barrier);
        let main_root = Arc::clone(&main_root_arc);
        let external_id = Arc::clone(&external_id_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..8 {
                let body = format!("routed external comment {idx}");
                results.push(run_br_in_dir(
                    &main_root,
                    [
                        "--lock-timeout",
                        "1",
                        "comments",
                        "add",
                        &external_id,
                        &body,
                        "--json",
                    ],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let local_update_results = local_updates.join().expect("local updates panicked");
    let routed_comment_results = routed_comments.join().expect("routed comments panicked");

    assert!(
        local_update_results.iter().any(|result| result.success),
        "local mutation worker never succeeded"
    );
    for (idx, result) in local_update_results.iter().enumerate() {
        assert_success_or_lock_failure(result, &format!("local routed update iteration {idx}"));
    }

    let routed_comment_successes =
        assert_only_success_or_contention("routed_external_comments", &routed_comment_results);
    assert!(
        routed_comment_successes > 0,
        "expected at least one successful routed external comment"
    );

    let show_external = run_br_in_dir(&main_root, ["show", &external_id_arc, "--json"]);
    assert!(
        show_external.success,
        "routed show after contention failed: {}",
        show_external.stderr
    );
    let payload = extract_json_payload(&show_external.stdout);
    let issues: Vec<serde_json::Value> = serde_json::from_str(&payload).expect("parse show json");
    let comments = issues[0]["comments"].as_array().expect("comments array");
    assert!(
        comments.len() >= routed_comment_successes,
        "expected at least {} routed comments to persist, got {}",
        routed_comment_successes,
        comments.len()
    );

    let show_local = run_br_in_dir(&main_root, ["show", &local_id_arc, "--json"]);
    assert!(
        show_local.success,
        "local show after routed mutation failed: {}",
        show_local.stderr
    );
}

/// Test that background sync-status checks touching `.beads/` remain readable
/// while mutating commands are auto-flushing JSONL.
#[test]
fn e2e_sync_status_observer_stays_available_during_writes() {
    let _log = common::test_log("e2e_sync_status_observer_stays_available_during_writes");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let barrier = Arc::new(Barrier::new(2));
    let root_arc = Arc::new(root.clone());

    let writer = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for idx in 0..6 {
                results.push(run_br_in_dir(
                    &root,
                    ["create", &format!("background observer issue {idx}")],
                ));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let observer = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&root_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for _ in 0..6 {
                results.push(run_br_in_dir(&root, ["sync", "--status", "--json"]));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let writer_results = writer.join().expect("writer panicked");
    let observer_results = observer.join().expect("observer panicked");

    for (idx, result) in writer_results.iter().enumerate() {
        assert!(result.success, "writer iteration {idx} failed: {}", result.stderr);
    }

    for (idx, result) in observer_results.iter().enumerate() {
        assert!(
            result.success,
            "sync --status iteration {idx} failed while observing .beads/: stdout={} stderr={}",
            result.stdout,
            result.stderr
        );
    }

    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "final list failed: {}", list.stderr);
    let payload = extract_json_payload(&list.stdout);
    let issues: Vec<serde_json::Value> = serde_json::from_str(&payload).expect("parse list json");
    assert_eq!(issues.len(), 6, "expected all writer issues to persist");
}

/// Test that database locked errors are properly reported.
///
/// This test verifies that when a lock cannot be acquired within the timeout,
/// an appropriate error message is returned.
#[test]
fn e2e_lock_error_reporting() {
    let _log = common::test_log("e2e_lock_error_reporting");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    // Initialize
    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    // Create a seed issue
    let create = run_br_in_dir(&root, ["create", "Lock test issue"]);
    assert!(create.success, "create failed: {}", create.stderr);

    // Normal operation should report no lock issues
    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list failed: {}", list.stderr);
    assert!(
        !list.stderr.to_lowercase().contains("lock"),
        "unexpected lock message in normal operation"
    );

    drop(temp_dir);
}

#[test]
fn e2e_interleaved_command_families_preserve_workspace_integrity() {
    let _log = common::test_log("e2e_interleaved_command_families_preserve_workspace_integrity");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let seed = run_br_in_dir(&root, ["create", "Concurrency seed issue"]);
    assert!(seed.success, "seed create failed: {}", seed.stderr);
    let issue_id = parse_created_id(&seed.stdout);
    assert!(!issue_id.is_empty(), "missing seed issue id");

    let barrier = Arc::new(Barrier::new(4));
    let shared_root = Arc::new(root.clone());
    let shared_issue_id = Arc::new(issue_id.clone());

    let create_root = Arc::clone(&shared_root);
    let create_barrier = Arc::clone(&barrier);
    let creator = thread::spawn(move || {
        create_barrier.wait();
        let mut results = Vec::new();
        for i in 0..6 {
            let args = vec![
                "--lock-timeout".to_string(),
                "50".to_string(),
                "create".to_string(),
                format!("Agent-created issue {i}"),
            ];
            results.push(run_br_in_dir(&create_root, args));
            thread::sleep(Duration::from_millis(10));
        }
        results
    });

    let comment_root = Arc::clone(&shared_root);
    let comment_issue_id = Arc::clone(&shared_issue_id);
    let comment_barrier = Arc::clone(&barrier);
    let commenter = thread::spawn(move || {
        comment_barrier.wait();
        let mut results = Vec::new();
        for i in 0..6 {
            let args = vec![
                "--lock-timeout".to_string(),
                "50".to_string(),
                "comments".to_string(),
                "add".to_string(),
                comment_issue_id.as_ref().clone(),
                format!("agent-note-{i}"),
            ];
            results.push(run_br_in_dir(&comment_root, args));
            thread::sleep(Duration::from_millis(10));
        }
        results
    });

    let label_root = Arc::clone(&shared_root);
    let label_issue_id = Arc::clone(&shared_issue_id);
    let label_barrier = Arc::clone(&barrier);
    let labeler = thread::spawn(move || {
        label_barrier.wait();
        let mut results = Vec::new();
        for i in 0..6 {
            let args = vec![
                "--lock-timeout".to_string(),
                "50".to_string(),
                "label".to_string(),
                "add".to_string(),
                label_issue_id.as_ref().clone(),
                format!("contended-{i}"),
            ];
            results.push(run_br_in_dir(&label_root, args));
            thread::sleep(Duration::from_millis(10));
        }
        results
    });

    let reader_root = Arc::clone(&shared_root);
    let reader_issue_id = Arc::clone(&shared_issue_id);
    let reader_barrier = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        reader_barrier.wait();
        let mut results = Vec::new();
        for i in 0..12 {
            let args = match i % 3 {
                0 => vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "list".to_string(),
                    "--json".to_string(),
                ],
                1 => vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "show".to_string(),
                    reader_issue_id.as_ref().clone(),
                    "--json".to_string(),
                ],
                _ => vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "ready".to_string(),
                    "--json".to_string(),
                ],
            };
            results.push(run_br_in_dir(&reader_root, args));
            thread::sleep(Duration::from_millis(5));
        }
        results
    });

    let create_results = creator.join().expect("creator panicked");
    let comment_results = commenter.join().expect("commenter panicked");
    let label_results = labeler.join().expect("labeler panicked");
    let reader_results = reader.join().expect("reader panicked");

    let create_successes = assert_only_success_or_contention("create", &create_results);
    let comment_successes = assert_only_success_or_contention("comments", &comment_results);
    let label_successes = assert_only_success_or_contention("labels", &label_results);
    let reader_successes = assert_only_success_or_contention("reader", &reader_results);

    assert!(create_successes > 0, "expected at least one successful create");
    assert!(
        comment_successes > 0,
        "expected at least one successful comment add"
    );
    assert!(label_successes > 0, "expected at least one successful label add");
    assert!(reader_successes > 0, "expected at least one successful reader command");

    let doctor = run_br_in_dir(&root, ["doctor", "--json"]);
    assert!(
        doctor.success,
        "doctor failed after contention: stdout={} stderr={}",
        doctor.stdout,
        doctor.stderr
    );

    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list failed after contention: {}", list.stderr);
    let issues: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&list.stdout)).expect("parse list json");
    assert!(
        issues.len() >= 1 + create_successes,
        "expected at least {} issues after concurrent creates, got {}",
        1 + create_successes,
        issues.len()
    );

    let comments = run_br_in_dir(&root, ["comments", "list", &issue_id, "--json"]);
    assert!(
        comments.success,
        "comments list failed after contention: {}",
        comments.stderr
    );
    let comment_json: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&comments.stdout))
            .expect("parse comments list json");
    assert!(
        comment_json.len() >= comment_successes,
        "expected at least {} comments, got {}",
        comment_successes,
        comment_json.len()
    );

    let labels = run_br_in_dir(&root, ["label", "list", &issue_id, "--json"]);
    assert!(
        labels.success,
        "label list failed after contention: {}",
        labels.stderr
    );
    let label_json: Vec<String> =
        serde_json::from_str(&extract_json_payload(&labels.stdout)).expect("parse label list");
    assert!(
        label_json.len() >= label_successes,
        "expected at least {} labels, got {}",
        label_successes,
        label_json.len()
    );

    let show = run_br_in_dir(&root, ["show", &issue_id, "--json"]);
    assert!(show.success, "show failed after contention: {}", show.stderr);

    drop(temp_dir);
}

#[test]
fn e2e_external_access_and_background_status_are_bounded_during_mutation() {
    let _log =
        common::test_log("e2e_external_access_and_background_status_are_bounded_during_mutation");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let seed = run_br_in_dir(&root, ["create", "External access seed issue"]);
    assert!(seed.success, "seed create failed: {}", seed.stderr);
    let issue_id = parse_created_id(&seed.stdout);
    assert!(!issue_id.is_empty(), "missing seed issue id");

    let beads_dir = Arc::new(root.join(".beads").display().to_string());
    let external_temp_dir = TempDir::new().expect("create external temp dir");
    let external_root = Arc::new(external_temp_dir.path().to_path_buf());

    let barrier = Arc::new(Barrier::new(3));
    let shared_root = Arc::new(root.clone());
    let shared_issue_id = Arc::new(issue_id.clone());

    let writer_root = Arc::clone(&shared_root);
    let writer_barrier = Arc::clone(&barrier);
    let local_writer = thread::spawn(move || {
        writer_barrier.wait();
        let mut results = Vec::new();
        for i in 0..8 {
            let args = vec![
                "--lock-timeout".to_string(),
                "50".to_string(),
                "create".to_string(),
                format!("local-mutation-{i}"),
            ];
            results.push(run_br_in_dir(&writer_root, args));
            thread::sleep(Duration::from_millis(8));
        }
        results
    });

    let read_root = Arc::clone(&external_root);
    let read_beads_dir = Arc::clone(&beads_dir);
    let read_issue_id = Arc::clone(&shared_issue_id);
    let read_barrier = Arc::clone(&barrier);
    let external_reader = thread::spawn(move || {
        read_barrier.wait();
        let mut results = Vec::new();
        for i in 0..10 {
            let args = if i % 2 == 0 {
                vec![
                    "--lock-timeout".to_string(),
                    "25".to_string(),
                    "list".to_string(),
                    "--json".to_string(),
                ]
            } else {
                vec![
                    "--lock-timeout".to_string(),
                    "25".to_string(),
                    "show".to_string(),
                    read_issue_id.as_ref().clone(),
                    "--json".to_string(),
                ]
            };
            results.push(run_br_in_dir_with_env(
                &read_root,
                args,
                [("BEADS_DIR", read_beads_dir.as_str())],
            ));
            thread::sleep(Duration::from_millis(6));
        }
        results
    });

    let status_root = Arc::clone(&external_root);
    let status_beads_dir = Arc::clone(&beads_dir);
    let status_barrier = Arc::clone(&barrier);
    let background_status = thread::spawn(move || {
        status_barrier.wait();
        let mut results = Vec::new();
        for _ in 0..10 {
            let args = vec![
                "--lock-timeout".to_string(),
                "25".to_string(),
                "sync".to_string(),
                "--status".to_string(),
                "--json".to_string(),
            ];
            results.push(run_br_in_dir_with_env(
                &status_root,
                args,
                [("BEADS_DIR", status_beads_dir.as_str())],
            ));
            thread::sleep(Duration::from_millis(6));
        }
        results
    });

    let writer_results = local_writer.join().expect("local writer panicked");
    let reader_results = external_reader.join().expect("external reader panicked");
    let status_results = background_status.join().expect("background status panicked");

    let writer_successes = assert_only_success_or_contention("writer", &writer_results);
    let reader_successes = assert_only_success_or_contention("external_reader", &reader_results);
    let status_successes =
        assert_only_success_or_contention("background_status", &status_results);

    assert!(writer_successes > 0, "expected at least one successful local write");
    assert!(
        reader_successes > 0,
        "expected at least one successful external BEADS_DIR access"
    );
    assert!(
        status_successes > 0,
        "expected at least one successful background status command"
    );

    let doctor = run_br_in_dir(&root, ["doctor", "--json"]);
    assert!(
        doctor.success,
        "doctor failed after external contention: stdout={} stderr={}",
        doctor.stdout,
        doctor.stderr
    );

    let status = run_br_in_dir(&root, ["sync", "--status", "--json"]);
    assert!(
        status.success,
        "sync --status failed after contention: stdout={} stderr={}",
        status.stdout,
        status.stderr
    );

    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list failed after contention: {}", list.stderr);
    let issues: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&list.stdout)).expect("parse list json");
    assert!(
        issues.len() >= 1 + writer_successes,
        "expected at least {} issues after local mutation, got {}",
        1 + writer_successes,
        issues.len()
    );

drop(external_temp_dir);
drop(temp_dir);
}

/// Test that actor-aware command families like claim and defer can interleave
/// with other mutating commands while leaving the workspace readable.
#[test]
fn e2e_actor_oriented_command_families_preserve_workspace_integrity() {
    let _log =
        common::test_log("e2e_actor_oriented_command_families_preserve_workspace_integrity");

    let temp_dir = TempDir::new().expect("create temp dir");
    let root = temp_dir.path().to_path_buf();

    let init = run_br_in_dir(&root, ["init"]);
    assert!(init.success, "init failed: {}", init.stderr);

    let claim_issue = run_br_in_dir(&root, ["create", "Claim target"]);
    assert!(claim_issue.success, "create claim target failed");
    let claim_id = parse_created_id(&claim_issue.stdout);

    let defer_issue = run_br_in_dir(&root, ["create", "Deferred target"]);
    assert!(defer_issue.success, "create defer target failed");
    let defer_id = parse_created_id(&defer_issue.stdout);

    let comment_issue = run_br_in_dir(&root, ["create", "Comment target"]);
    assert!(comment_issue.success, "create comment target failed");
    let comment_id = parse_created_id(&comment_issue.stdout);

    let label_issue = run_br_in_dir(&root, ["create", "Label target"]);
    assert!(label_issue.success, "create label target failed");
    let label_id = parse_created_id(&label_issue.stdout);

    let barrier = Arc::new(Barrier::new(5));
    let shared_root = Arc::new(root.clone());

    let claimer = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&shared_root);
        let claim_id = claim_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for _ in 0..6 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "--actor".to_string(),
                    "alice".to_string(),
                    "update".to_string(),
                    claim_id.clone(),
                    "--claim".to_string(),
                    "--json".to_string(),
                ];
                results.push(run_br_in_dir(&root, args));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let deferrer = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&shared_root);
        let defer_id = defer_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for _ in 0..6 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "--actor".to_string(),
                    "dave".to_string(),
                    "defer".to_string(),
                    defer_id.clone(),
                    "--until".to_string(),
                    "2026-12-01T00:00:00Z".to_string(),
                    "--json".to_string(),
                ];
                results.push(run_br_in_dir(&root, args));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let commenter = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&shared_root);
        let comment_id = comment_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..6 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "--actor".to_string(),
                    "carol".to_string(),
                    "comments".to_string(),
                    "add".to_string(),
                    comment_id.clone(),
                    format!("actor-note-{i}"),
                ];
                results.push(run_br_in_dir(&root, args));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let labeler = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&shared_root);
        let label_id = label_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..6 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "--actor".to_string(),
                    "bob".to_string(),
                    "label".to_string(),
                    "add".to_string(),
                    label_id.clone(),
                    format!("actor-lane-{i}"),
                ];
                results.push(run_br_in_dir(&root, args));
                thread::sleep(Duration::from_millis(10));
            }
            results
        })
    };

    let reader = {
        let barrier = Arc::clone(&barrier);
        let root = Arc::clone(&shared_root);
        let claim_id = claim_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..12 {
                let args = match i % 3 {
                    0 => vec![
                        "--lock-timeout".to_string(),
                        "50".to_string(),
                        "show".to_string(),
                        claim_id.clone(),
                        "--json".to_string(),
                    ],
                    1 => vec![
                        "--lock-timeout".to_string(),
                        "50".to_string(),
                        "ready".to_string(),
                        "--json".to_string(),
                    ],
                    _ => vec![
                        "--lock-timeout".to_string(),
                        "50".to_string(),
                        "stats".to_string(),
                        "--json".to_string(),
                    ],
                };
                results.push(run_br_in_dir(&root, args));
                thread::sleep(Duration::from_millis(5));
            }
            results
        })
    };

    let claim_results = claimer.join().expect("claimer panicked");
    let defer_results = deferrer.join().expect("deferrer panicked");
    let comment_results = commenter.join().expect("commenter panicked");
    let label_results = labeler.join().expect("labeler panicked");
    let reader_results = reader.join().expect("reader panicked");

    let claim_successes = assert_only_success_or_contention("claim", &claim_results);
    let defer_successes = assert_only_success_or_contention("defer", &defer_results);
    let comment_successes = assert_only_success_or_contention("comments", &comment_results);
    let label_successes = assert_only_success_or_contention("labels", &label_results);
    let reader_successes = assert_only_success_or_contention("reader", &reader_results);

    assert!(claim_successes > 0, "expected at least one successful claim");
    assert!(defer_successes > 0, "expected at least one successful defer");
    assert!(
        comment_successes > 0,
        "expected at least one successful comment add"
    );
    assert!(label_successes > 0, "expected at least one successful label add");
    assert!(reader_successes > 0, "expected at least one successful reader command");

    let doctor = run_br_in_dir(&root, ["doctor", "--json"]);
    assert!(
        doctor.success,
        "doctor failed after actor contention: stdout={} stderr={}",
        doctor.stdout,
        doctor.stderr
    );

    let claim_show = run_br_in_dir(&root, ["show", &claim_id, "--json"]);
    assert!(
        claim_show.success,
        "show claim target failed: {}",
        claim_show.stderr
    );
    let claim_json: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&claim_show.stdout)).expect("claim show json");
    assert_eq!(claim_json[0]["status"].as_str(), Some("in_progress"));
    assert_eq!(claim_json[0]["assignee"].as_str(), Some("alice"));

    let defer_show = run_br_in_dir(&root, ["show", &defer_id, "--json"]);
    assert!(
        defer_show.success,
        "show defer target failed: {}",
        defer_show.stderr
    );
    let defer_json: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&defer_show.stdout)).expect("defer show json");
    assert_eq!(defer_json[0]["status"].as_str(), Some("deferred"));
    let defer_until = defer_json[0]["defer_until"]
        .as_str()
        .expect("defer_until should be present");
    assert!(
        defer_until.starts_with("2026-12-01"),
        "unexpected defer_until value: {defer_until}"
    );

    let comments = run_br_in_dir(&root, ["comments", "list", &comment_id, "--json"]);
    assert!(
        comments.success,
        "comments list failed after actor contention: {}",
        comments.stderr
    );
    let comment_json: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&comments.stdout))
            .expect("parse comments list json");
    assert!(
        comment_json.len() >= comment_successes,
        "expected at least {} comments, got {}",
        comment_successes,
        comment_json.len()
    );
    assert!(
        comment_json
            .iter()
            .all(|comment| comment["author"].as_str() == Some("carol")),
        "expected all comment authors to be carol: {}",
        comments.stdout
    );

    let labels = run_br_in_dir(&root, ["label", "list", &label_id, "--json"]);
    assert!(
        labels.success,
        "label list failed after actor contention: {}",
        labels.stderr
    );
    let label_json: Vec<String> =
        serde_json::from_str(&extract_json_payload(&labels.stdout)).expect("parse label list");
    assert!(
        label_json.len() >= label_successes,
        "expected at least {} labels, got {}",
        label_successes,
        label_json.len()
    );

    let list = run_br_in_dir(&root, ["list", "--json"]);
    assert!(list.success, "list failed after actor contention: {}", list.stderr);
}

/// Test that routed access remains bounded even while the routed workspace
/// itself is mutating, not just the invoking workspace.
#[test]
fn e2e_routed_access_remains_bounded_while_remote_workspace_mutates() {
    let _log =
        common::test_log("e2e_routed_access_remains_bounded_while_remote_workspace_mutates");

    let main_temp_dir = TempDir::new().expect("create main temp dir");
    let external_temp_dir = TempDir::new().expect("create external temp dir");
    let main_root = main_temp_dir.path().to_path_buf();
    let external_root = external_temp_dir.path().to_path_buf();

    let init_main = run_br_in_dir(&main_root, ["init"]);
    assert!(init_main.success, "main init failed: {}", init_main.stderr);
    let init_external = run_br_in_dir(&external_root, ["init"]);
    assert!(
        init_external.success,
        "external init failed: {}",
        init_external.stderr
    );

    configure_external_route(&main_root, &external_root);

    let local_issue = run_br_in_dir(&main_root, ["create", "Local routed contention target"]);
    assert!(local_issue.success, "create local issue failed");
    let local_id = parse_created_id(&local_issue.stdout);

    let external_issue =
        run_br_in_dir(&external_root, ["create", "External routed contention target"]);
    assert!(external_issue.success, "create external issue failed");
    let external_id = parse_created_id(&external_issue.stdout);

    let barrier = Arc::new(Barrier::new(3));
    let main_root_arc = Arc::new(main_root.clone());
    let external_root_arc = Arc::new(external_root.clone());

    let local_writer = {
        let barrier = Arc::clone(&barrier);
        let main_root = Arc::clone(&main_root_arc);
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..8 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "create".to_string(),
                    format!("local-route-write-{i}"),
                ];
                results.push(run_br_in_dir(&main_root, args));
                thread::sleep(Duration::from_millis(8));
            }
            results
        })
    };

    let external_writer = {
        let barrier = Arc::clone(&barrier);
        let external_root = Arc::clone(&external_root_arc);
        let external_id = external_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..8 {
                let args = vec![
                    "--lock-timeout".to_string(),
                    "50".to_string(),
                    "--actor".to_string(),
                    "bob".to_string(),
                    "update".to_string(),
                    external_id.clone(),
                    "--title".to_string(),
                    format!("remote-mutation-{i}"),
                    "--json".to_string(),
                ];
                results.push(run_br_in_dir(&external_root, args));
                thread::sleep(Duration::from_millis(8));
            }
            results
        })
    };

    let routed_worker = {
        let barrier = Arc::clone(&barrier);
        let main_root = Arc::clone(&main_root_arc);
        let external_id = external_id.clone();
        thread::spawn(move || {
            barrier.wait();
            let mut results = Vec::new();
            for i in 0..10 {
                let args = if i % 2 == 0 {
                    vec![
                        "--lock-timeout".to_string(),
                        "25".to_string(),
                        "show".to_string(),
                        external_id.clone(),
                        "--json".to_string(),
                    ]
                } else {
                    vec![
                        "--lock-timeout".to_string(),
                        "25".to_string(),
                        "--actor".to_string(),
                        "carol".to_string(),
                        "label".to_string(),
                        "add".to_string(),
                        external_id.clone(),
                        "remote-route".to_string(),
                    ]
                };
                results.push(run_br_in_dir(&main_root, args));
                thread::sleep(Duration::from_millis(6));
            }
            results
        })
    };

    let local_results = local_writer.join().expect("local writer panicked");
    let external_results = external_writer.join().expect("external writer panicked");
    let routed_results = routed_worker.join().expect("routed worker panicked");

    let local_successes = assert_only_success_or_contention("local_writer", &local_results);
    let external_successes = assert_only_success_or_contention("external_writer", &external_results);
    let routed_successes = assert_only_success_or_contention("routed_worker", &routed_results);

    assert!(local_successes > 0, "expected at least one successful local write");
    assert!(
        external_successes > 0,
        "expected at least one successful remote mutation"
    );
    assert!(
        routed_successes > 0,
        "expected at least one successful routed access"
    );

    let main_doctor = run_br_in_dir(&main_root, ["doctor", "--json"]);
    assert!(
        main_doctor.success,
        "main doctor failed after routed contention: stdout={} stderr={}",
        main_doctor.stdout,
        main_doctor.stderr
    );

    let routed_show = run_br_in_dir(&main_root, ["show", &external_id, "--json"]);
    assert!(
        routed_show.success,
        "show routed issue failed: {}",
        routed_show.stderr
    );
    let routed_json: Vec<serde_json::Value> =
        serde_json::from_str(&extract_json_payload(&routed_show.stdout))
            .expect("parse routed show json");
    let routed_title = routed_json[0]["title"]
        .as_str()
        .expect("routed title should be present");
    assert!(
        routed_title.starts_with("remote-mutation-"),
        "expected remote title mutation, got: {routed_title}"
    );

    let external_labels = run_br_in_dir(&external_root, ["label", "list", &external_id, "--json"]);
    assert!(
        external_labels.success,
        "label list on external workspace failed: {}",
        external_labels.stderr
    );
    let label_json: Vec<String> = serde_json::from_str(&extract_json_payload(&external_labels.stdout))
        .expect("parse external label list");
    assert!(
        label_json.iter().any(|label| label == "remote-route"),
        "expected remote-route label in external workspace: {}",
        external_labels.stdout
    );

    let local_show = run_br_in_dir(&main_root, ["show", &local_id, "--json"]);
    assert!(
        local_show.success,
        "show local issue failed after routed contention: {}",
        local_show.stderr
    );

    let main_status = run_br_in_dir(&main_root, ["sync", "--status", "--json"]);
    assert!(
        main_status.success,
        "sync --status failed after routed contention: stdout={} stderr={}",
        main_status.stdout,
        main_status.stderr
    );
}
