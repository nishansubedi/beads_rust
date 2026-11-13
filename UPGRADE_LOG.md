# Dependency Upgrade Log

**Date:** 2026-01-18  |  **Project:** beads_rust  |  **Language:** Rust

## Summary
- **Updated:** 4  |  **Skipped:** 1  |  **Failed:** 0  |  **Needs attention:** 0

## Updates

### criterion: 0.7.0 -> 0.8.1
- **Breaking:** None documented; version bump aligns criterion-plot dependency
- **Migration:** None required
- **Tests:** Passed (649)

### rusqlite: 0.32.1 -> 0.38.0
- **Breaking:** `usize`/`u64` ToSql/FromSql disabled by default; statement cache optional; min SQLite 3.34.1
- **Migration:** Added `fallible_uint` feature flag to re-enable `usize` ToSql support
- **Tests:** Passed (649)

### unicode-width: 0.1.14 -> 0.2.2
- **Breaking:** Control characters now return `Some(1)` instead of `None`
- **Migration:** Code already uses `unwrap_or(0)` which handles the change gracefully
- **Tests:** Passed (649)

### indicatif: 0.17.11 -> 0.18.3
- **Breaking:** None found in project usage
- **Tests:** Passed (649)

---

## Skipped

### vergen-gix: 1.0.9 -> 9.1.0
- **Reason:** Blocked by Rust version constraint; vergen-gix 9.x may require newer Rust than 1.85
- **Action:** Investigate if project can bump rust-version in rust-toolchain.toml, or wait for compat release

---

## Transitive Updates (via cargo update)

These were automatically updated as dependencies of direct dependencies:
- hashlink: 0.9.1 -> 0.11.0 (rusqlite dependency)
- libsqlite3-sys: 0.30.1 -> 0.36.0 (rusqlite dependency)
- criterion-plot: 0.6.0 -> 0.8.1 (criterion dependency)
- Various gix-* crates (vergen-gix dependencies)

---

## Commands Used

```bash
# Check for outdated dependencies
cargo outdated

# Update specific package
cargo update -p rusqlite

# Run tests after each update
cargo test --lib
```

---
---

# Upgrade Session: 2026-02-19

**Date:** 2026-02-19  |  **Project:** beads_rust  |  **Language:** Rust

## Summary
- **Updated:** 24  |  **Replaced:** 1 (serde_yaml -> serde_yml)  |  **Skipped:** 4  |  **Failed:** 0

## Toolchain

### Rust nightly: unversioned -> nightly-2026-02-19
- **Change:** Pinned `rust-toolchain.toml` from generic `nightly` to `nightly-2026-02-19` (rustc 1.95.0-nightly, 2026-02-18)
- **Reason:** Reproducible builds; generic `nightly` channel was corrupted on this machine
- **Tests:** All pass

## Major Updates (Breaking Changes)

### schemars: 0.8 -> 1.0
- **Breaking:** `RootSchema` removed (replaced by `Schema`), `schema_name()` returns `Cow<'static, str>` instead of `String`, `schemars::gen` module renamed to `schemars::generate`, feature `chrono` renamed to `chrono04`, schemas now target JSON Schema draft 2020-12
- **Migration:** Updated Cargo.toml features, replaced `RootSchema` with `Schema` in `schema.rs`, updated manual `JsonSchema` impl in `model/mod.rs` to use new trait signatures
- **Files changed:** `Cargo.toml`, `src/model/mod.rs`, `src/cli/commands/schema.rs`
- **Tests:** Pass

### serde_yaml 0.9 -> serde_yml 0.0.12 (REPLACED)
- **Reason:** `serde_yaml` is deprecated and unmaintained; `serde_yml` is a maintained fork with compatible API
- **Migration:** Renamed all `serde_yaml::` references to `serde_yml::` across 4 source files. Fixed a `match_same_arms` clippy lint exposed by the reformatting
- **Files changed:** `Cargo.toml`, `src/error/mod.rs`, `src/config/mod.rs`, `src/cli/commands/config.rs`, `tests/conformance.rs`
- **Tests:** Pass

### crossterm: 0.28 -> 0.29
- **Breaking:** `KeyModifiers` display now uses `+` separators; new default `derive-more` feature
- **Impact:** This project only uses `crossterm::style::Stylize` -- no impact
- **Tests:** Pass

### rand: 0.9.2 -> 0.10
- **Breaking:** `Rng` extension trait renamed to `RngExt`, `StdRng` no longer implements `Clone`, `SeedableRng::from_os_rng()` removed
- **Migration:** Updated `use rand::Rng` to `use rand::RngExt` in `tests/bench_synthetic_scale.rs`
- **Files changed:** `Cargo.toml`, `tests/bench_synthetic_scale.rs`
- **Tests:** Pass

## Minor/Patch Updates (No Breaking Changes)

| Crate | From | To | Notes |
|-------|------|----|-------|
| clap | 4.5 | 4.5.60 | Semver-compatible |
| clap_complete | 4.5 | 4.5.66 | Semver-compatible |
| serde | 1.0 | 1.0.228 | Semver-compatible |
| serde_json | 1.0 | 1.0.149 | Semver-compatible |
| chrono | 0.4 | 0.4.43 | Semver-compatible |
| sha2 | 0.10 | 0.10.9 | Semver-compatible |
| anyhow | 1.0 | 1.0.102 | Semver-compatible |
| tracing | 0.1 | 0.1.44 | Semver-compatible |
| tracing-subscriber | 0.3 | 0.3.22 | Semver-compatible |
| indicatif | 0.18 | 0.18.4 | Semver-compatible |
| dunce | 1.0 | 1.0.5 | Semver-compatible |
| once_cell | 1.19 | 1.21 | Semver-compatible |
| regex | 1.11 | 1.12 | Semver-compatible |
| unicode-width | 0.2 | 0.2.2 | Semver-compatible |
| semver | 1.0 | 1.0.27 | Semver-compatible |
| tempfile | 3.10 | 3.25 | Semver-compatible |
| assert_cmd | 2.0 | 2.1 | Semver-compatible |
| predicates | 3.1 | 3.1.4 | Semver-compatible |
| criterion | 0.8 | 0.8.2 | Semver-compatible |
| walkdir | 2.4 | 2.5 | Semver-compatible |
| insta | 1.38 | 1.46 | Semver-compatible |
| proptest | 1.6 | 1.10 | Semver-compatible |

## Skipped

| Crate | Version | Reason |
|-------|---------|--------|
| rusqlite | 0.38 | Already at latest stable |
| thiserror | 2.0.18 | Already at latest stable |
| rich_rust | 0.2.0 | Already at latest stable |
| toon_rust | git:788589d | Git dependency, preserving pinned revision |

## Validation

- `cargo check --all-targets`: Pass
- `cargo clippy --all-targets -- -D warnings`: Pass
- `cargo fmt --check`: Pass
- `cargo test --lib`: 775 passed, 0 failed
- `cargo test --test conformance`: 320 passed, 0 failed, 26 ignored
- `cargo test` (full): 1 pre-existing failure in `e2e_close_blocked_requires_force` (unrelated to upgrades)

---
---

# Upgrade Session: 2026-11-12

**Date:** 2026-11-12  |  **Project:** beads_rust 0.1.29  |  **Language:** Rust

### rusqlite: 0.38.0 -> 0.42.0 (bundled SQLite 3.51.1 -> 3.54.1)
- **Why:** SQLite 3.51.0 and 3.51.1 deadlock a process whose threads open and close the same WAL
  file at once: a lock-order inversion between the unix VFS's global mutex and a file's inode
  mutex (`unixLock` -> `unixIsSharingShmNode` vs `findReusableFd`), fixed in 3.51.2. An
  embedding application (muninn) can't move its own rusqlite past 0.38 while this crate holds
  it there, since only one crate in a build may link `sqlite3`.
- **Breaking:** none in this crate's usage. It compiled unchanged; no warnings.
- **rust-version:** 1.88 -> 1.89, rusqlite 0.41's MSRV.
- **Transitive:** libsqlite3-sys 0.36.0 -> 0.40.0, hashlink 0.11.0 -> 0.12.2, thiserror
  2.0.18 -> 2.0.21; wasm-only: sqlite-wasm-rs 0.5.2 -> 0.6.3, rsqlite-vfs 0.1.0 -> 0.2.0.
- **Tests:** `cargo test --lib -- --test-threads=1`: 1078 passed, 34 failed. The same 34 fail
  at ed68f7f on rusqlite 0.38 (macOS temp-dir symlinks, cwd-dependent fixtures, and others).
  The storage_*, jsonl_import_export and history_backups_switch suites: the same pass and
  fail counts before and after.

---
---

# Upgrade Session: 2026-11-13

**Date:** 2026-11-13  |  **Project:** beads_rust 0.1.29 -> 0.1.30  |  **Language:** Rust

No dependency changes. Two import/export data-loss fixes, for heim bead bd-16b1o.92.

### Import: the content-hash match no longer folds a new issue onto a twin the JSONL carries
- **Bug:** `detect_collision`'s phase 3 matched an incoming issue with a new id to the first
  local issue with the same content hash, recorded a rename, and remapped the incoming issue,
  its dependency sources and targets and its comment owners onto that local id. muninn writes
  many issues with identical content (a workflow's run beads, repeated step beads), so a new
  run from another machine never reached the database: if newer it overwrote the twin's row,
  labels, dependencies and comments; if older it was skipped. The overwrite (DELETE+INSERT)
  moved the twin to the end of the table, so each import picked the next twin. On one Mac:
  52 JSONL issues missing from beads.db, 36 rows carrying another issue's timestamps.
- **Fix:** phase 3 applies only when the matched local id is not itself in the incoming JSONL.
  A local issue the JSONL still carries under its own id was not renamed. A local issue the
  JSONL no longer carries can still be matched, as before (the rename case).
- **Kept:** upstream's displaced-id case (#512): an issue moved to a new id while its old id
  holds another issue is inserted at the new id, not folded back onto the old one.
- **Not a schema change:** schema version stays 4; no migration.
- **Tests (src/sync/mod.rs):** `test_import_keeps_a_newer_identical_content_bead_from_another_machine`,
  `test_import_keeps_an_older_identical_content_bead_from_another_machine`,
  `test_reimport_with_identical_content_beads_is_idempotent`,
  `test_import_does_not_fold_a_displaced_issue_back_onto_its_old_id`,
  `test_detect_collision_skips_content_hash_match_on_an_id_the_jsonl_carries`. The four import
  tests fail with the old phase 3 and pass with the fix.

### Export: `needs_flush` no longer turns off the data-loss guards (#405, upstream beabee6d)
- **Bug:** `auto_flush`, `sync --flush-only` and the no-db flush passed `needs_flush` as
  `force`. Only a hard delete (`purge_issue`) sets `needs_flush`, so the flush after a hard
  delete skipped both guards ("Refusing to export empty database", "Refusing to export stale
  database") and rewrote the JSONL from the database, erasing every JSONL issue the database
  had never imported.
- **Fix, ported from beabee6d (rewritten for this fork's sync module):** `force` is only the
  user's `--force`. `purge_issue` records the purged id in metadata
  (`purged_ids_pending_export`) in the same transaction; both guards leave those ids out of
  their loss check; a full export clears the marker. The incremental auto-flush leaves it,
  since it rewrites only dirty lines and runs only without `needs_flush`.
- **Tests:** `test_flush_after_hard_delete_keeps_jsonl_issues_the_database_never_imported`
  (hard delete, flush, count: the flush refuses and the JSONL keeps all 3 issues; fails with
  the old `force`), `test_flush_after_hard_delete_prunes_only_the_purged_issue`,
  `test_flush_after_purging_every_issue_writes_an_empty_jsonl`, and
  `tests/e2e_sync_needs_flush_guard.rs` through the CLI (its refusal test fails with the old
  `force`).

### Validation
- `cargo test --lib -- sync:: storage:: config:: --test-threads=1`: 409 passed, 23 failed; at
  9f5c8b4, 401 passed and the same 23 failed (macOS temp-dir symlinks, cwd-dependent fixtures).
- `jsonl_import_export`: 156 passed, 2 failed, the same 2 as at 9f5c8b4 (the shared
  `common::dataset_registry` fixtures). `history_backups_switch`, `repro_collision_labels`:
  pass. `e2e_sync_needs_flush_guard`: 2 passed.
- `cargo fmt --check` and `cargo clippy` report nothing in the changed lines; both already
  fail elsewhere at 9f5c8b4.
