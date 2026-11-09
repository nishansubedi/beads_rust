//! `set_backups_by_default(false)` stops every default export from copying the old
//! `issues.jsonl` into `.br_history/`. Its own test binary: the switch is process-wide.

use beads_rust::sync::history::{HistoryConfig, backup_before_export, set_backups_by_default};
use std::fs;

#[test]
fn turning_backups_off_leaves_no_history() {
    let dir = tempfile::tempdir().unwrap();
    let beads_dir = dir.path();
    let jsonl = beads_dir.join("issues.jsonl");
    fs::write(&jsonl, "{\"id\":\"bd-1\"}\n").unwrap();

    assert!(
        HistoryConfig::default().enabled,
        "on unless an application turns it off"
    );
    backup_before_export(beads_dir, &HistoryConfig::default(), &jsonl).unwrap();
    let copies = |d: &std::path::Path| {
        fs::read_dir(d.join(".br_history"))
            .map(|it| {
                it.filter_map(Result::ok)
                    .filter(|e| e.path().extension().is_some_and(|x| x == "jsonl"))
                    .count()
            })
            .unwrap_or(0)
    };
    assert_eq!(copies(beads_dir), 1, "the default keeps a copy");

    set_backups_by_default(false);
    fs::write(&jsonl, "{\"id\":\"bd-2\"}\n").unwrap();
    assert!(!HistoryConfig::default().enabled);
    backup_before_export(beads_dir, &HistoryConfig::default(), &jsonl).unwrap();
    assert_eq!(copies(beads_dir), 1, "no new copy once backups are off");
    set_backups_by_default(true);
}
