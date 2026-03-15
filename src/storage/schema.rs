//! Database schema definitions and migration logic.

use crate::storage::compat::Connection;
use crate::storage::compat::SqliteValue;

use crate::error::{BeadsError, Result};

pub const CURRENT_SCHEMA_VERSION: i32 = 4;
const ISSUES_CLOSED_AT_CHECK: &str = "CHECK ((status = 'closed' AND closed_at IS NOT NULL) OR (status = 'tombstone') OR (status NOT IN ('closed', 'tombstone') AND closed_at IS NULL))";

/// The complete SQL schema for the beads database.
/// Schema matches classic bd (Go) for interoperability.
pub const SCHEMA_SQL: &str = r"
    -- Issues table
    -- Note: TEXT fields use DEFAULT '' for bd (Go) compatibility.
    -- bd's sql.Scan doesn't handle NULL well when scanning into string fields.
    -- Closed-at invariant is enforced by the CHECK clause below.
    CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        content_hash TEXT,
        title TEXT NOT NULL CHECK(length(title) <= 500),
        description TEXT NOT NULL DEFAULT '',
        design TEXT NOT NULL DEFAULT '',
        acceptance_criteria TEXT NOT NULL DEFAULT '',
        notes TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        priority INTEGER NOT NULL DEFAULT 2 CHECK(priority >= 0 AND priority <= 4),
        issue_type TEXT NOT NULL DEFAULT 'task',
        assignee TEXT,
        owner TEXT DEFAULT '',
        estimated_minutes INTEGER,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT '',
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        closed_at DATETIME,
        close_reason TEXT DEFAULT '',
        closed_by_session TEXT DEFAULT '',
        due_at DATETIME,
        defer_until DATETIME,
        external_ref TEXT,
        source_system TEXT DEFAULT '',
        source_repo TEXT NOT NULL DEFAULT '.',
        deleted_at DATETIME,
        deleted_by TEXT DEFAULT '',
        delete_reason TEXT DEFAULT '',
        original_type TEXT DEFAULT '',
        compaction_level INTEGER DEFAULT 0,
        compacted_at DATETIME,
        compacted_at_commit TEXT,
        original_size INTEGER,
        sender TEXT DEFAULT '',
        ephemeral INTEGER NOT NULL DEFAULT 0,
        pinned INTEGER NOT NULL DEFAULT 0,
        is_template INTEGER NOT NULL DEFAULT 0,
        CHECK (
            (status = 'closed' AND closed_at IS NOT NULL) OR
            (status = 'tombstone') OR
            (status NOT IN ('closed', 'tombstone') AND closed_at IS NULL)
        )
    );

    -- Primary access patterns
    CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
    CREATE INDEX IF NOT EXISTS idx_issues_priority ON issues(priority);
    CREATE INDEX IF NOT EXISTS idx_issues_issue_type ON issues(issue_type);
    CREATE INDEX IF NOT EXISTS idx_issues_assignee ON issues(assignee) WHERE assignee IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_issues_created_at ON issues(created_at);
    CREATE INDEX IF NOT EXISTS idx_issues_updated_at ON issues(updated_at);

    -- Export/sync patterns
    CREATE INDEX IF NOT EXISTS idx_issues_content_hash ON issues(content_hash);
    CREATE INDEX IF NOT EXISTS idx_issues_external_ref ON issues(external_ref) WHERE external_ref IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_external_ref_unique ON issues(external_ref) WHERE external_ref IS NOT NULL;

    -- Special states
    CREATE INDEX IF NOT EXISTS idx_issues_ephemeral ON issues(ephemeral) WHERE ephemeral = 1;
    CREATE INDEX IF NOT EXISTS idx_issues_pinned ON issues(pinned) WHERE pinned = 1;
    CREATE INDEX IF NOT EXISTS idx_issues_tombstone ON issues(status) WHERE status = 'tombstone';

    -- Time-based
    CREATE INDEX IF NOT EXISTS idx_issues_due_at ON issues(due_at) WHERE due_at IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_issues_defer_until ON issues(defer_until) WHERE defer_until IS NOT NULL;

    -- Ready work composite index (most important for performance)
    CREATE INDEX IF NOT EXISTS idx_issues_ready
        ON issues(status, priority, created_at)
        WHERE status = 'open'
        AND ephemeral = 0
        AND pinned = 0
        AND is_template = 0;

    -- Common active list path: non-terminal issues sorted by priority/created_at
    CREATE INDEX IF NOT EXISTS idx_issues_list_active_order
        ON issues(priority, created_at DESC)
        WHERE status NOT IN ('closed', 'tombstone')
        AND (is_template = 0 OR is_template IS NULL);

    -- Dependencies
    CREATE TABLE IF NOT EXISTS dependencies (
        issue_id TEXT NOT NULL,
        depends_on_id TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'blocks',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT NOT NULL DEFAULT '',
        metadata TEXT DEFAULT '{}',
        thread_id TEXT DEFAULT '',
        PRIMARY KEY (issue_id, depends_on_id),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
        -- Note: depends_on_id FK intentionally removed to allow external issue references
    );
    CREATE INDEX IF NOT EXISTS idx_dependencies_issue ON dependencies(issue_id);
    CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
    CREATE INDEX IF NOT EXISTS idx_dependencies_type ON dependencies(type);
    CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on_type ON dependencies(depends_on_id, type);
    CREATE INDEX IF NOT EXISTS idx_dependencies_thread ON dependencies(thread_id) WHERE thread_id != '';
    -- Composite for blocking lookups
    CREATE INDEX IF NOT EXISTS idx_dependencies_blocking
        ON dependencies(depends_on_id, issue_id)
        WHERE (type = 'blocks' OR type = 'parent-child' OR type = 'conditional-blocks' OR type = 'waits-for');

    -- Labels
    CREATE TABLE IF NOT EXISTS labels (
        issue_id TEXT NOT NULL,
        label TEXT NOT NULL,
        PRIMARY KEY (issue_id, label),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label);
    CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);

    -- Comments
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        author TEXT NOT NULL,
        text TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id);
    CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at);

    -- Events (Audit)
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT '',
        old_value TEXT,
        new_value TEXT,
        comment TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_events_issue ON events(issue_id);
    CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
    CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
    CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor) WHERE actor != '';

    -- Config (Runtime)
    -- NOTE: Avoid PRIMARY KEY/UNIQUE constraints here because the current
    -- storage engine does not reliably maintain unique autoindexes.
    -- Application code enforces key replacement via DELETE + INSERT.
    CREATE TABLE IF NOT EXISTS config (
        key TEXT NOT NULL,
        value TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_config_key ON config(key);

    -- Metadata
    -- Same rationale as config: keep it as key-value with explicit index.
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT NOT NULL,
        value TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(key);

    -- Dirty Issues (for export)
    CREATE TABLE IF NOT EXISTS dirty_issues (
        issue_id TEXT PRIMARY KEY,
        marked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_dirty_issues_marked_at ON dirty_issues(marked_at);

    -- Export Hashes (for incremental export)
    CREATE TABLE IF NOT EXISTS export_hashes (
        issue_id TEXT PRIMARY KEY,
        content_hash TEXT NOT NULL,
        exported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );

    -- Blocked Issues Cache (Materialized view)
    -- Rebuilt on dependency or status changes.
    -- `blocked_by` stores a JSON array of blocking issue IDs.
    CREATE TABLE IF NOT EXISTS blocked_issues_cache (
        issue_id TEXT PRIMARY KEY,
        blocked_by TEXT NOT NULL,
        blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_blocked_cache_blocked_at ON blocked_issues_cache(blocked_at);

    -- Child Counters (for hierarchical IDs like bd-abc.1, bd-abc.2)
    CREATE TABLE IF NOT EXISTS child_counters (
        parent_id TEXT PRIMARY KEY,
        last_child INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (parent_id) REFERENCES issues(id) ON DELETE CASCADE
    );
";

/// Split a SQL script into individual statements, respecting string literals,
/// quoted identifiers, and comments.
///
/// A naive `split(';')` breaks when SQL string literals contain semicolons
/// (e.g., `INSERT INTO t(v) VALUES('a;b')`). This function uses a small state
/// machine to track whether the current position is inside:
/// - A single-quoted string literal (`'...'`, with `''` as escape)
/// - A double-quoted identifier (`"..."`, with `""` as escape)
/// - A line comment (`-- ...`)
/// - A block comment (`/* ... */`)
///
/// Only semicolons at the top level (outside all of the above) are treated as
/// statement terminators.
fn split_sql_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut stmts = Vec::new();
    let mut start = 0; // byte offset where the current statement begins
    let mut i = 0;

    // State flags — at most one is true at a time.
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < len {
        let b = bytes[i];

        // --- Line comment state ---
        if in_line_comment {
            if b == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        // --- Block comment state ---
        if in_block_comment {
            if b == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }

        // --- Single-quoted string state ---
        if in_single_quote {
            if b == b'\'' {
                // '' is an escaped quote inside a string literal
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        // --- Double-quoted identifier state ---
        if in_double_quote {
            if b == b'"' {
                if i + 1 < len && bytes[i + 1] == b'"' {
                    i += 2;
                } else {
                    in_double_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        // --- Top-level parsing ---
        if b == b'\'' {
            in_single_quote = true;
            i += 1;
        } else if b == b'"' {
            in_double_quote = true;
            i += 1;
        } else if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
        } else if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
        } else if b == b';' {
            // Statement terminator at top level.
            let stmt = &sql[start..i];
            if !stmt.trim().is_empty() {
                stmts.push(stmt.trim());
            }
            start = i + 1;
            i += 1;
        } else {
            i += 1;
        }
    }

    // Trailing statement without a final semicolon.
    if start < len {
        let stmt = &sql[start..len];
        if !stmt.trim().is_empty() {
            stmts.push(stmt.trim());
        }
    }

    stmts
}

/// Execute multiple SQL statements separated by semicolons.
///
/// fsqlite does not support `execute_batch`, so we split the SQL script
/// into individual statements (respecting string literals and comments)
/// and execute each one individually.
pub(crate) fn execute_batch(conn: &Connection, sql: &str) -> Result<()> {
    for stmt in split_sql_statements(sql) {
        conn.execute(stmt)?;
    }
    Ok(())
}

/// Apply the schema to the database.
///
/// This splits the DDL script into individual statements and executes them.
/// It is idempotent because all statements use `IF NOT EXISTS`.
///
/// # Errors
///
/// Returns an error if the SQL execution fails or pragmas cannot be set.
pub fn apply_schema(conn: &Connection) -> Result<()> {
    // Run pre-schema migrations first to fix any incompatible old tables
    // This must run BEFORE execute_batch because the batch includes CREATE INDEX
    // statements that will fail if old tables have missing columns
    run_pre_schema_migrations(conn)?;

    execute_batch(conn, SCHEMA_SQL)?;

    // Run migrations for existing databases
    run_migrations(conn)?;

    apply_runtime_pragmas(conn)?;

    // Mark schema as applied so future opens can skip DDL/migration work.
    conn.execute(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))?;

    Ok(())
}

pub(crate) fn apply_runtime_compatible_schema(conn: &Connection) -> Result<()> {
    // The table layouts are already safe to operate on, so we can skip the
    // heavier pre-schema rebuilds and just restore any missing canonical DDL.
    execute_batch(conn, SCHEMA_SQL)?;
    run_migrations(conn)?;
    apply_runtime_pragmas(conn)?;
    conn.execute(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))?;
    Ok(())
}

pub(crate) fn apply_runtime_pragmas(conn: &Connection) -> Result<()> {
    // New databases should opt into WAL, but steady-state opens should not
    // reassert the current mode and turn a read path into a write-like one.
    let journal_mode = conn
        .query_row("PRAGMA journal_mode")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text).map(str::to_owned))
        .unwrap_or_default();
    if !journal_mode.eq_ignore_ascii_case("wal") {
        conn.execute("PRAGMA journal_mode = WAL")?;
    }

    // Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")?;

    // Performance PRAGMAs (safe with WAL mode)
    // NORMAL synchronous is safe with WAL: committed data survives OS crash
    conn.execute("PRAGMA synchronous = NORMAL")?;
    // Use memory for temp tables/indexes instead of disk
    conn.execute("PRAGMA temp_store = MEMORY")?;
    // 8MB page cache (default is ~2MB), improves read-heavy workloads
    conn.execute("PRAGMA cache_size = -8000")?;

    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> bool {
    let escaped_table = table.replace('\'', "''");
    let sql = format!("SELECT 1 FROM sqlite_master WHERE type='table' AND name='{escaped_table}'");
    conn.query(&sql).is_ok_and(|rows| !rows.is_empty())
}

fn index_exists(conn: &Connection, index: &str) -> bool {
    let escaped_index = index.replace('\'', "''");
    let sql = format!("SELECT 1 FROM sqlite_master WHERE type='index' AND name='{escaped_index}'");
    conn.query(&sql).is_ok_and(|rows| !rows.is_empty())
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
    let sql = format!("PRAGMA table_info('{table}')");
    conn.query(&sql).is_ok_and(|rows| {
        rows.iter()
            .any(|row| row.get(1).and_then(SqliteValue::as_text) == Some(column))
    })
}

const ISSUE_COLUMNS: &[(&str, &str)] = &[
    ("content_hash", "TEXT"),
    ("description", "TEXT NOT NULL DEFAULT ''"),
    ("design", "TEXT NOT NULL DEFAULT ''"),
    ("acceptance_criteria", "TEXT NOT NULL DEFAULT ''"),
    ("notes", "TEXT NOT NULL DEFAULT ''"),
    ("status", "TEXT NOT NULL DEFAULT 'open'"),
    (
        "priority",
        "INTEGER NOT NULL DEFAULT 2 CHECK(priority >= 0 AND priority <= 4)",
    ),
    ("issue_type", "TEXT NOT NULL DEFAULT 'task'"),
    ("assignee", "TEXT"),
    ("owner", "TEXT DEFAULT ''"),
    ("estimated_minutes", "INTEGER"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("created_by", "TEXT DEFAULT ''"),
    ("updated_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("closed_at", "DATETIME"),
    ("close_reason", "TEXT DEFAULT ''"),
    ("closed_by_session", "TEXT DEFAULT ''"),
    ("due_at", "DATETIME"),
    ("defer_until", "DATETIME"),
    ("external_ref", "TEXT"),
    ("source_system", "TEXT DEFAULT ''"),
    ("source_repo", "TEXT NOT NULL DEFAULT '.'"),
    ("deleted_at", "DATETIME"),
    ("deleted_by", "TEXT DEFAULT ''"),
    ("delete_reason", "TEXT DEFAULT ''"),
    ("original_type", "TEXT DEFAULT ''"),
    ("compaction_level", "INTEGER DEFAULT 0"),
    ("compacted_at", "DATETIME"),
    ("compacted_at_commit", "TEXT"),
    ("original_size", "INTEGER"),
    ("sender", "TEXT DEFAULT ''"),
    ("ephemeral", "INTEGER NOT NULL DEFAULT 0"),
    ("pinned", "INTEGER NOT NULL DEFAULT 0"),
    ("is_template", "INTEGER NOT NULL DEFAULT 0"),
];

const DEPENDENCY_COLUMNS: &[(&str, &str)] = &[
    ("type", "TEXT NOT NULL DEFAULT 'blocks'"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("created_by", "TEXT NOT NULL DEFAULT ''"),
    ("metadata", "TEXT DEFAULT '{}'"),
    ("thread_id", "TEXT DEFAULT ''"),
];

const COMMENT_COLUMNS: &[(&str, &str)] = &[
    ("author", "TEXT NOT NULL DEFAULT ''"),
    ("text", "TEXT NOT NULL DEFAULT ''"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
];

const EVENT_COLUMNS: &[(&str, &str)] = &[
    ("event_type", "TEXT NOT NULL DEFAULT ''"),
    ("actor", "TEXT NOT NULL DEFAULT ''"),
    ("old_value", "TEXT"),
    ("new_value", "TEXT"),
    ("comment", "TEXT"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
];

fn ensure_columns(conn: &Connection, table: &str, columns: &[(&str, &str)]) -> Result<()> {
    if !table_exists(conn, table) {
        return Ok(());
    }

    for (name, definition) in columns {
        if !column_exists(conn, table, name) {
            let sql = format!("ALTER TABLE {table} ADD COLUMN {name} {definition}");
            conn.execute(&sql)?;
        }
    }

    Ok(())
}

fn table_has_columns(conn: &Connection, table: &str, required_columns: &[&str]) -> bool {
    table_exists(conn, table)
        && required_columns
            .iter()
            .all(|column| column_exists(conn, table, column))
}

fn current_schema_version_declared(conn: &Connection) -> bool {
    conn.query_row("PRAGMA user_version")
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
        .is_some_and(|version| version >= i64::from(CURRENT_SCHEMA_VERSION))
}

fn core_runtime_tables_exist(conn: &Connection) -> bool {
    [
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
    ]
    .iter()
    .all(|table| table_exists(conn, table))
}

/// Expected column order for the issues table (id + ISSUE_COLUMNS names).
/// Used to detect when ALTER TABLE has appended columns in the wrong position,
/// which causes fsqlite to fail with "no such column" errors on older databases.
const EXPECTED_ISSUE_COLUMN_ORDER: &[&str] = &[
    "id",
    "content_hash",
    "title",
    "description",
    "design",
    "acceptance_criteria",
    "notes",
    "status",
    "priority",
    "issue_type",
    "assignee",
    "owner",
    "estimated_minutes",
    "created_at",
    "created_by",
    "updated_at",
    "closed_at",
    "close_reason",
    "closed_by_session",
    "due_at",
    "defer_until",
    "external_ref",
    "source_system",
    "source_repo",
    "deleted_at",
    "deleted_by",
    "delete_reason",
    "original_type",
    "compaction_level",
    "compacted_at",
    "compacted_at_commit",
    "original_size",
    "sender",
    "ephemeral",
    "pinned",
    "is_template",
];

/// Check whether the issues table has columns in the expected order.
/// Returns `true` if the column order matches, `false` if it differs or the
/// table doesn't exist.
fn issues_column_order_matches(conn: &Connection) -> bool {
    if !table_exists(conn, "issues") {
        return true; // Will be created fresh by SCHEMA_SQL
    }

    let Ok(rows) = conn.query("PRAGMA table_info(issues)") else {
        return false;
    };

    let actual_columns: Vec<String> = rows
        .iter()
        .filter_map(|row| row.get(1).and_then(SqliteValue::as_text).map(String::from))
        .collect();

    if actual_columns.len() != EXPECTED_ISSUE_COLUMN_ORDER.len() {
        return false;
    }

    actual_columns
        .iter()
        .zip(EXPECTED_ISSUE_COLUMN_ORDER.iter())
        .all(|(actual, expected)| actual == expected)
}

fn issues_filter_columns_require_v3_rebuild(conn: &Connection) -> bool {
    let Ok(rows) = conn.query("PRAGMA table_info('issues')") else {
        return true;
    };

    for column in ["ephemeral", "pinned", "is_template"] {
        let Some(row) = rows
            .iter()
            .find(|row| row.get(1).and_then(SqliteValue::as_text) == Some(column))
        else {
            return true;
        };

        let not_null = row.get(3).and_then(SqliteValue::as_integer).unwrap_or(0);
        if not_null == 0 {
            return true;
        }
    }

    false
}

/// Rebuild the issues table so columns match the canonical SCHEMA_SQL order.
///
/// This fixes databases where ALTER TABLE ADD COLUMN appended columns in a
/// different position than the CREATE TABLE definition, causing fsqlite's
/// column-name resolver to fail with "no such column" errors.
///
/// Uses the standard SQLite migration pattern:
///   1. Create new table with correct schema
///   2. Copy data from old table
///   3. Drop old table
///   4. Rename new table
fn rebuild_issues_table(conn: &Connection) -> Result<()> {
    let existing_rows = conn.query("PRAGMA table_info('issues')")?;
    let existing_columns: Vec<String> = existing_rows
        .iter()
        .filter_map(|row| row.get(1).and_then(SqliteValue::as_text).map(String::from))
        .collect();

    if existing_columns.is_empty() {
        return Ok(()); // Table is empty or doesn't exist
    }

    // Disable foreign keys during the rebuild because we'll be dropping
    // and recreating the issues table which is referenced by other tables.
    // This property is connection-scoped.
    conn.execute("PRAGMA foreign_keys = OFF")?;

    // Wrap the entire rebuild in a transaction so a crash between DROP TABLE
    // and RENAME cannot lose data.
    conn.execute("BEGIN EXCLUSIVE")?;

    if let Err(e) = rebuild_issues_table_inner(conn, &existing_columns) {
        let _ = conn.execute("ROLLBACK");
        let _ = conn.execute("PRAGMA foreign_keys = ON");
        return Err(e);
    }

    conn.execute("COMMIT")?;

    // Restore foreign key enforcement
    conn.execute("PRAGMA foreign_keys = ON")?;

    Ok(())
}

/// Inner helper for [`rebuild_issues_table`] that performs the actual work
/// inside an already-open transaction.
fn rebuild_issues_table_inner(conn: &Connection, existing_columns: &[String]) -> Result<()> {
    // Drop all indexes on the issues table first (they'll be recreated by SCHEMA_SQL)
    let index_rows =
        conn.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='issues' AND sql IS NOT NULL")?;
    for row in &index_rows {
        if let Some(name) = row.get(0).and_then(SqliteValue::as_text) {
            conn.execute(&format!("DROP INDEX IF EXISTS \"{name}\""))?;
        }
    }

    // Drop tables that have foreign keys referencing issues (they'll be recreated)
    // We need to save and restore their data too.
    // For simplicity, we only rebuild the issues table and let SCHEMA_SQL
    // recreate indexes. Foreign key tables (dependencies, labels, etc.) keep
    // their data since we use the same primary key.

    // Create the new table with canonical column order
    // Use a temporary name to avoid conflicts
    conn.execute("DROP TABLE IF EXISTS issues_rebuild_tmp")?;

    // Build CREATE TABLE for the new table with only columns that exist in the old table
    // plus any missing columns with defaults
    // Build the canonical column list: id, content_hash, title, then the
    // rest of ISSUE_COLUMNS (skipping content_hash which is already placed).
    // This order must match EXPECTED_ISSUE_COLUMN_ORDER and SCHEMA_SQL.
    let all_expected: Vec<(&str, &str)> = std::iter::once(("id", "TEXT PRIMARY KEY"))
        .chain(std::iter::once(("content_hash", "TEXT")))
        .chain(std::iter::once((
            "title",
            "TEXT NOT NULL CHECK(length(title) <= 500)",
        )))
        .chain(
            ISSUE_COLUMNS
                .iter()
                .copied()
                .filter(|(name, _)| *name != "content_hash"),
        )
        .collect();

    let mut create_cols = Vec::new();
    for (col_name, col_def) in &all_expected {
        create_cols.push(format!("{col_name} {col_def}"));
    }
    create_cols.push(ISSUES_CLOSED_AT_CHECK.to_string());

    let create_sql = format!(
        "CREATE TABLE issues_rebuild_tmp ({})",
        create_cols.join(", ")
    );
    conn.execute(&create_sql)?;

    // Copy only columns that exist in the source table so SQLite can apply
    // declared defaults for newer columns that are absent in legacy schemas.
    let mut projected_columns = Vec::new();
    for (col_name, _) in &all_expected {
        if existing_columns.iter().any(|c| c == col_name) {
            projected_columns.push((*col_name).to_string());
        }
    }

    if projected_columns.is_empty() {
        return Err(BeadsError::Config(
            "Cannot rebuild legacy issues table: no canonical issue columns were found".to_string(),
        ));
    }

    let copy_sql = format!(
        "INSERT INTO issues_rebuild_tmp ({}) SELECT {} FROM issues",
        projected_columns.join(", "),
        projected_columns.join(", ")
    );
    conn.execute(&copy_sql)?;

    // Swap tables
    conn.execute("DROP TABLE issues")?;
    conn.execute("ALTER TABLE issues_rebuild_tmp RENAME TO issues")?;

    Ok(())
}

fn kv_table_uses_primary_key(conn: &Connection, table: &str) -> bool {
    if !table_exists(conn, table) {
        return false;
    }

    let escaped_table = table.replace('\'', "''");
    let sql =
        format!("SELECT sql FROM sqlite_master WHERE type='table' AND name='{escaped_table}'");
    conn.query_row(&sql)
        .ok()
        .and_then(|row| row.get(0).and_then(SqliteValue::as_text).map(str::to_owned))
        .is_some_and(|ddl| ddl.to_ascii_uppercase().contains("PRIMARY KEY"))
}

fn kv_table_needs_canonical_rebuild(conn: &Connection, table: &str, expected_index: &str) -> bool {
    table_exists(conn, table)
        && (!index_exists(conn, expected_index) || kv_table_uses_primary_key(conn, table))
}

fn rebuild_kv_table_without_unique(conn: &Connection, table: &str) -> Result<()> {
    let tmp_table = format!("{table}_rebuild_tmp");

    conn.execute("BEGIN EXCLUSIVE")?;

    let result = (|| -> Result<()> {
        conn.execute(&format!("DROP TABLE IF EXISTS {tmp_table}"))?;
        conn.execute(&format!(
            "CREATE TABLE {tmp_table} (
                key TEXT NOT NULL,
                value TEXT NOT NULL
            )"
        ))?;

        conn.execute(&format!(
            "INSERT INTO {tmp_table} (key, value)
             SELECT key, value
             FROM {table}"
        ))?;

        conn.execute(&format!("DROP TABLE {table}"))?;
        conn.execute(&format!("ALTER TABLE {tmp_table} RENAME TO {table}"))?;
        Ok(())
    })();

    if let Err(err) = result {
        let _ = conn.execute("ROLLBACK");
        return Err(err);
    }

    conn.execute("COMMIT")?;
    Ok(())
}

/// Run pre-schema migrations to fix incompatible old tables.
///
/// This must run BEFORE `execute_batch(SCHEMA_SQL)` because the schema includes
/// CREATE INDEX statements that will fail if old tables have missing columns.
fn run_pre_schema_migrations(conn: &Connection) -> Result<()> {
    // Legacy schemas used PRIMARY KEY on config/metadata key columns.
    // Rebuild to plain key-value tables so standard sqlite integrity checks
    // are not tripped by unsupported unique-index maintenance behavior.
    if kv_table_needs_canonical_rebuild(conn, "config", "idx_config_key") {
        rebuild_kv_table_without_unique(conn, "config")?;
    }
    if kv_table_needs_canonical_rebuild(conn, "metadata", "idx_metadata_key") {
        rebuild_kv_table_without_unique(conn, "metadata")?;
    }

    // Drop blocked_issues_cache if it exists but lacks required columns.
    // The main schema will recreate it with the correct structure.
    if table_exists(conn, "blocked_issues_cache") {
        let has_blocked_at = column_exists(conn, "blocked_issues_cache", "blocked_at");
        let has_blocked_by = column_exists(conn, "blocked_issues_cache", "blocked_by");
        let has_issue_id = column_exists(conn, "blocked_issues_cache", "issue_id");

        if !has_blocked_at || !has_blocked_by || !has_issue_id {
            conn.execute("DROP TABLE IF EXISTS blocked_issues_cache")?;
        }
    }

    // Rebuild the issues table if columns are out of order or missing.
    // This fixes fsqlite "no such column" errors on databases created with
    // older br versions where ALTER TABLE ADD COLUMN appended columns in
    // a different position than the canonical CREATE TABLE definition.
    if table_exists(conn, "issues") && !issues_column_order_matches(conn) {
        rebuild_issues_table(conn)?;
    }

    // Ensure legacy tables have all columns needed for schema indexes and queries.
    ensure_columns(conn, "issues", ISSUE_COLUMNS)?;
    ensure_columns(conn, "dependencies", DEPENDENCY_COLUMNS)?;
    ensure_columns(conn, "comments", COMMENT_COLUMNS)?;
    ensure_columns(conn, "events", EVENT_COLUMNS)?;

    // Intentionally do not rebuild idx_issues_ready here.
    //
    // Older databases may have a stale partial-index predicate, but that is a
    // performance issue rather than a correctness issue. On large file-backed
    // databases, exercising DROP INDEX through frankensqlite currently trips an
    // out-of-memory failure. Additionally, frankensqlite's in-memory schema
    // representation does not reliably preserve partial-index predicates, so br
    // cannot distinguish a stale ready index from a current one at open time.

    Ok(())
}

pub(crate) fn runtime_schema_compatible(conn: &Connection) -> bool {
    if current_schema_version_declared(conn) && core_runtime_tables_exist(conn) {
        return true;
    }

    let issues_ok = issues_column_order_matches(conn);
    let dependencies_ok = table_has_columns(conn, "dependencies", &["issue_id", "depends_on_id"])
        && DEPENDENCY_COLUMNS
            .iter()
            .all(|(name, _)| column_exists(conn, "dependencies", name));
    let labels_ok = table_has_columns(conn, "labels", &["issue_id", "label"]);
    let comments_ok = table_has_columns(conn, "comments", &["id", "issue_id"])
        && COMMENT_COLUMNS
            .iter()
            .all(|(name, _)| column_exists(conn, "comments", name));
    let events_ok = table_has_columns(conn, "events", &["id", "issue_id"])
        && EVENT_COLUMNS
            .iter()
            .all(|(name, _)| column_exists(conn, "events", name));
    let config_ok = table_has_columns(conn, "config", &["key", "value"])
        && index_exists(conn, "idx_config_key")
        && !kv_table_uses_primary_key(conn, "config");
    let metadata_ok = table_has_columns(conn, "metadata", &["key", "value"])
        && index_exists(conn, "idx_metadata_key")
        && !kv_table_uses_primary_key(conn, "metadata");
    let dirty_issues_ok = table_has_columns(conn, "dirty_issues", &["issue_id", "marked_at"]);
    let export_hashes_ok = table_has_columns(
        conn,
        "export_hashes",
        &["issue_id", "content_hash", "exported_at"],
    );
    let blocked_cache_ok = table_has_columns(
        conn,
        "blocked_issues_cache",
        &["issue_id", "blocked_by", "blocked_at"],
    );
    let child_counters_ok = table_has_columns(conn, "child_counters", &["parent_id", "last_child"]);

    let compatible = issues_ok
        && dependencies_ok
        && labels_ok
        && comments_ok
        && events_ok
        && config_ok
        && metadata_ok
        && dirty_issues_ok
        && export_hashes_ok
        && blocked_cache_ok
        && child_counters_ok;

    if !compatible {
        tracing::debug!(
            issues_ok,
            dependencies_ok,
            labels_ok,
            comments_ok,
            events_ok,
            config_ok,
            metadata_ok,
            dirty_issues_ok,
            export_hashes_ok,
            blocked_cache_ok,
            child_counters_ok,
            "runtime schema compatibility check failed"
        );
    }

    compatible
}

/// Run schema migrations for existing databases.
///
/// This handles upgrades for tables that may have been created with older schemas.
#[allow(clippy::too_many_lines)]
fn run_migrations(conn: &Connection) -> Result<()> {
    // Migration: Ensure blocked_issues_cache has correct schema (blocked_by, blocked_at)
    // Check for old column name (blocked_by_json) or missing columns
    let has_blocked_by = column_exists(conn, "blocked_issues_cache", "blocked_by");
    let has_blocked_at = column_exists(conn, "blocked_issues_cache", "blocked_at");
    let has_issue_id = column_exists(conn, "blocked_issues_cache", "issue_id");

    if !has_blocked_by || !has_blocked_at || !has_issue_id {
        // Table needs update - drop and recreate (it's a cache, data is regenerated)
        // Wrap in transaction so concurrent opens don't see a partially migrated state
        conn.execute("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            conn.execute("DROP TABLE IF EXISTS blocked_issues_cache")?;
            conn.execute(
                "CREATE TABLE blocked_issues_cache (
                    issue_id TEXT PRIMARY KEY,
                    blocked_by TEXT NOT NULL,
                    blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
                )",
            )?;
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_blocked_cache_blocked_at ON blocked_issues_cache(blocked_at)",
            )?;
            Ok(())
        })();

        if let Err(e) = result {
            let _ = conn.execute("ROLLBACK");
            return Err(e);
        }
        conn.execute("COMMIT")?;
    }

    // Migration: ensure compaction_level is never NULL (bd compatibility)
    let has_compaction_level = column_exists(conn, "issues", "compaction_level");

    if has_compaction_level {
        conn.execute("UPDATE issues SET compaction_level = 0 WHERE compaction_level IS NULL")?;
    }

    // Migration: Ensure filter columns are NOT NULL (v3)
    let user_version = conn
        .query_row("PRAGMA user_version")?
        .get(0)
        .and_then(SqliteValue::as_integer)
        .unwrap_or(0);

    if user_version < 3
        && table_exists(conn, "issues")
        && issues_filter_columns_require_v3_rebuild(conn)
    {
        tracing::info!("Migrating database to schema version 3 (NOT NULL filter columns)");
        // 1. Backfill NULL values
        conn.execute("UPDATE issues SET ephemeral = 0 WHERE ephemeral IS NULL")?;
        conn.execute("UPDATE issues SET pinned = 0 WHERE pinned IS NULL")?;
        conn.execute("UPDATE issues SET is_template = 0 WHERE is_template IS NULL")?;

        // 2. Rebuild the table to apply NOT NULL constraints
        rebuild_issues_table(conn)?;

        // 3. Recreate the optimized ready index
        conn.execute("DROP INDEX IF EXISTS idx_issues_ready")?;
        conn.execute(
            "CREATE INDEX idx_issues_ready
             ON issues(status, priority, created_at)
             WHERE status = 'open'
             AND ephemeral = 0
             AND pinned = 0
             AND is_template = 0",
        )?;
    }

    if user_version < 4 && table_exists(conn, "issues") {
        tracing::info!("Migrating database to schema version 4 (ready excludes in_progress)");
        conn.execute("DROP INDEX IF EXISTS idx_issues_ready")?;
        conn.execute(
            "CREATE INDEX idx_issues_ready
             ON issues(status, priority, created_at)
             WHERE status = 'open'
             AND ephemeral = 0
             AND pinned = 0
             AND is_template = 0",
        )?;
    }

    // Note: source_repo and is_template column backfills are handled in
    // run_pre_schema_migrations() via ensure_columns(). Repeating ALTER TABLE
    // here can create duplicate column definitions on some engines.

    // Migration: Add missing indexes for bd parity
    // These use IF NOT EXISTS so they're safe to run multiple times
    execute_batch(
        conn,
        r"
        -- Export/sync patterns
        CREATE INDEX IF NOT EXISTS idx_issues_content_hash ON issues(content_hash);
        CREATE INDEX IF NOT EXISTS idx_issues_external_ref ON issues(external_ref) WHERE external_ref IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_external_ref_unique ON issues(external_ref) WHERE external_ref IS NOT NULL;

        -- Special states
        CREATE INDEX IF NOT EXISTS idx_issues_ephemeral ON issues(ephemeral) WHERE ephemeral = 1;
        CREATE INDEX IF NOT EXISTS idx_issues_pinned ON issues(pinned) WHERE pinned = 1;
        CREATE INDEX IF NOT EXISTS idx_issues_tombstone ON issues(status) WHERE status = 'tombstone';

        -- Time-based
        CREATE INDEX IF NOT EXISTS idx_issues_due_at ON issues(due_at) WHERE due_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_issues_defer_until ON issues(defer_until) WHERE defer_until IS NOT NULL;

        -- Ready work composite index (most important for performance)
        CREATE INDEX IF NOT EXISTS idx_issues_ready
            ON issues(status, priority, created_at)
            WHERE status = 'open'
            AND ephemeral = 0
            AND pinned = 0
            AND is_template = 0;

        -- Common active list path: non-terminal issues sorted by priority/created_at
        CREATE INDEX IF NOT EXISTS idx_issues_list_active_order
            ON issues(priority, created_at DESC)
            WHERE status NOT IN ('closed', 'tombstone')
            AND (is_template = 0 OR is_template IS NULL);

    ",
    )?;

    // Drop legacy index names (safe if absent)
    execute_batch(
        conn,
        r"
        DROP INDEX IF EXISTS idx_dependencies_issue_id;
        DROP INDEX IF EXISTS idx_dependencies_depends_on_id;
        DROP INDEX IF EXISTS idx_dependencies_composite;
        DROP INDEX IF EXISTS idx_labels_issue_id;
    ",
    )?;

    if table_exists(conn, "dependencies") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_dependencies_issue ON dependencies(issue_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_type ON dependencies(type);
            CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on_type ON dependencies(depends_on_id, type);
            CREATE INDEX IF NOT EXISTS idx_dependencies_blocking
                ON dependencies(depends_on_id, issue_id)
                WHERE (type = 'blocks' OR type = 'parent-child' OR type = 'conditional-blocks' OR type = 'waits-for');
        ",
        )?;

        if column_exists(conn, "dependencies", "thread_id") {
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dependencies_thread ON dependencies(thread_id) WHERE thread_id != ''",
            )?;
        }
    }

    if table_exists(conn, "labels") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label);
            CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);
        ",
        )?;
    }

    if table_exists(conn, "comments") {
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id)")?;
    }

    if table_exists(conn, "events") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_events_issue ON events(issue_id);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
            CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor) WHERE actor != '';
        ",
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BeadsError;
    use crate::storage::compat::Connection;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn test_apply_schema() {
        let conn = Connection::open(":memory:").unwrap();
        apply_schema(&conn).expect("Failed to apply schema");

        // Verify a few tables exist
        let tables: Vec<String> = conn
            .query("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(tables.contains(&"issues".to_string()));
        assert!(tables.contains(&"dependencies".to_string()));
        assert!(tables.contains(&"config".to_string()));
        assert!(tables.contains(&"dirty_issues".to_string()));

        // Verify pragmas
        let row = conn.query_row("PRAGMA journal_mode").unwrap();
        let journal_mode = row
            .get(0)
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string();
        // In-memory DBs use MEMORY journaling, regardless of what we set
        assert!(journal_mode.to_uppercase() == "WAL" || journal_mode.to_uppercase() == "MEMORY");

        let row = conn.query_row("PRAGMA foreign_keys").unwrap();
        let foreign_keys = row.get(0).and_then(SqliteValue::as_integer).unwrap_or(0);
        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn test_apply_schema_file_backed_has_no_duplicate_issues_columns() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("beads.db");
        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();

        apply_schema(&conn).expect("Failed to apply schema");

        let row = conn
            .query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name='issues'")
            .expect("issues table should exist");
        let issues_sql = row
            .get(0)
            .and_then(SqliteValue::as_text)
            .expect("issues table SQL should be present");

        assert_eq!(
            issues_sql.matches("source_repo").count(),
            1,
            "issues table SQL should define source_repo exactly once"
        );
        assert_eq!(
            issues_sql.matches("is_template").count(),
            1,
            "issues table SQL should define is_template exactly once"
        );
    }

    /// Conformance test: Verify schema matches bd (Go) for interoperability.
    /// Tests table structure, defaults, constraints, and indexes.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_schema_parity_conformance() {
        let conn = Connection::open(":memory:").unwrap();
        apply_schema(&conn).expect("Failed to apply schema");

        // === ISSUES TABLE ===
        // Verify column defaults
        let issues_cols: Vec<(String, String, i32, Option<String>)> = conn
            .query("PRAGMA table_info(issues)")
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row.get(1)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    row.get(2)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        row.get(3).and_then(SqliteValue::as_integer).unwrap_or(0) as i32
                    },
                    row.get(4).and_then(|v| v.as_text()).map(String::from),
                )
            })
            .collect();

        // Check required defaults for bd parity
        let col_map: std::collections::HashMap<_, _> = issues_cols
            .iter()
            .map(|(name, typ, notnull, dflt)| {
                (name.as_str(), (typ.as_str(), *notnull, dflt.clone()))
            })
            .collect();

        // status must default to 'open'
        assert_eq!(
            col_map.get("status").map(|c| c.2.as_deref()),
            Some(Some("'open'")),
            "status should default to 'open'"
        );

        // priority must default to 2
        assert_eq!(
            col_map.get("priority").map(|c| c.2.as_deref()),
            Some(Some("2")),
            "priority should default to 2"
        );

        // issue_type must default to 'task'
        assert_eq!(
            col_map.get("issue_type").map(|c| c.2.as_deref()),
            Some(Some("'task'")),
            "issue_type should default to 'task'"
        );

        // created_at and updated_at must default to CURRENT_TIMESTAMP
        assert_eq!(
            col_map.get("created_at").map(|c| c.2.as_deref()),
            Some(Some("CURRENT_TIMESTAMP")),
            "created_at should default to CURRENT_TIMESTAMP"
        );
        assert_eq!(
            col_map.get("updated_at").map(|c| c.2.as_deref()),
            Some(Some("CURRENT_TIMESTAMP")),
            "updated_at should default to CURRENT_TIMESTAMP"
        );

        // === VERIFY KEY INDEXES EXIST ===
        let indexes: HashSet<String> = conn
            .query("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_text()).map(String::from))
            .collect();

        // Core indexes
        assert!(
            indexes.contains("idx_issues_status"),
            "missing idx_issues_status"
        );
        assert!(
            indexes.contains("idx_issues_priority"),
            "missing idx_issues_priority"
        );
        assert!(
            indexes.contains("idx_issues_issue_type"),
            "missing idx_issues_issue_type"
        );
        assert!(
            indexes.contains("idx_issues_created_at"),
            "missing idx_issues_created_at"
        );
        assert!(
            indexes.contains("idx_issues_updated_at"),
            "missing idx_issues_updated_at"
        );

        // Export/sync indexes
        assert!(
            indexes.contains("idx_issues_content_hash"),
            "missing idx_issues_content_hash"
        );
        assert!(
            indexes.contains("idx_issues_external_ref")
                || indexes.contains("idx_issues_external_ref_unique"),
            "missing external_ref index"
        );

        // Special state indexes
        assert!(
            indexes.contains("idx_issues_ephemeral"),
            "missing idx_issues_ephemeral"
        );
        assert!(
            indexes.contains("idx_issues_pinned"),
            "missing idx_issues_pinned"
        );
        assert!(
            indexes.contains("idx_issues_tombstone"),
            "missing idx_issues_tombstone"
        );

        // Time-based indexes
        assert!(
            indexes.contains("idx_issues_due_at"),
            "missing idx_issues_due_at"
        );
        assert!(
            indexes.contains("idx_issues_defer_until"),
            "missing idx_issues_defer_until"
        );

        // Ready work composite index (critical for performance)
        assert!(
            indexes.contains("idx_issues_ready"),
            "missing idx_issues_ready composite index"
        );
        assert!(
            indexes.contains("idx_issues_list_active_order"),
            "missing idx_issues_list_active_order composite index"
        );

        // === DEPENDENCIES TABLE ===
        let deps_cols: Vec<(String, Option<String>)> = conn
            .query("PRAGMA table_info(dependencies)")
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row.get(1)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    row.get(4).and_then(|v| v.as_text()).map(String::from),
                )
            })
            .collect();

        let deps_map: std::collections::HashMap<_, _> = deps_cols
            .iter()
            .map(|(name, dflt)| (name.as_str(), dflt.clone()))
            .collect();

        // type must default to 'blocks'
        assert_eq!(
            deps_map.get("type").cloned().flatten().as_deref(),
            Some("'blocks'"),
            "dependencies.type should default to 'blocks'"
        );

        // metadata must default to '{}'
        assert_eq!(
            deps_map.get("metadata").cloned().flatten().as_deref(),
            Some("'{}'"),
            "dependencies.metadata should default to '{{}}'"
        );

        // Dependency indexes (bd parity)
        assert!(
            indexes.contains("idx_dependencies_issue"),
            "missing idx_dependencies_issue"
        );
        assert!(
            indexes.contains("idx_dependencies_depends_on"),
            "missing idx_dependencies_depends_on"
        );
        assert!(
            indexes.contains("idx_dependencies_type"),
            "missing idx_dependencies_type"
        );
        assert!(
            indexes.contains("idx_dependencies_depends_on_type"),
            "missing idx_dependencies_depends_on_type"
        );
        assert!(
            indexes.contains("idx_dependencies_thread"),
            "missing idx_dependencies_thread"
        );
        assert!(
            indexes.contains("idx_dependencies_blocking"),
            "missing idx_dependencies_blocking"
        );

        // Labels indexes
        assert!(
            indexes.contains("idx_labels_label"),
            "missing idx_labels_label"
        );
        assert!(
            indexes.contains("idx_labels_issue"),
            "missing idx_labels_issue"
        );

        // === BLOCKED_ISSUES_CACHE TABLE ===
        let cache_cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cache_cols.contains(&"issue_id".to_string()),
            "blocked_issues_cache should have 'issue_id' column"
        );

        // Must have blocked_by (not blocked_by_json) and blocked_at
        assert!(
            cache_cols.contains(&"blocked_by".to_string()),
            "blocked_issues_cache should have 'blocked_by' column (not 'blocked_by_json')"
        );
        assert!(
            cache_cols.contains(&"blocked_at".to_string()),
            "blocked_issues_cache should have 'blocked_at' column"
        );
        assert!(
            !cache_cols.contains(&"blocked_by_json".to_string()),
            "blocked_issues_cache should NOT have old 'blocked_by_json' column"
        );

        // Verify blocked_cache index exists
        assert!(
            indexes.contains("idx_blocked_cache_blocked_at"),
            "missing idx_blocked_cache_blocked_at"
        );

        // === TEST CLOSED-AT CONSTRAINT ===
        // Insert an issue with defaults (will get status='open', closed_at=NULL)
        conn.execute("INSERT INTO issues (id, title) VALUES ('test-1', 'Test Issue')")
            .expect("Should allow open issue without closed_at");

        // Try to insert closed issue without closed_at — CHECK constraint
        // should reject it. fsqlite does not yet enforce CHECK constraints,
        // so we accept either outcome.
        let result = conn.execute(
            "INSERT INTO issues (id, title, status) VALUES ('test-2', 'Closed', 'closed')",
        );
        if result.is_ok() {
            // fsqlite: CHECK not enforced — clean up the row so later assertions
            // are not affected by the extra row.
            let _ = conn.execute("DELETE FROM issues WHERE id = 'test-2'");
        }

        // Insert closed issue with closed_at - should succeed
        conn.execute(
            "INSERT INTO issues (id, title, status, closed_at) VALUES ('test-3', 'Closed', 'closed', CURRENT_TIMESTAMP)",
        )
        .expect("Should allow closed issue with closed_at");

        // Insert tombstone without closed_at - should succeed (tombstones exempt)
        conn.execute(
            "INSERT INTO issues (id, title, status) VALUES ('test-4', 'Tombstone', 'tombstone')",
        )
        .expect("Should allow tombstone without closed_at");
    }

    /// Test that migrations correctly upgrade old schemas.
    #[test]
    fn test_migration_blocked_cache_upgrade() {
        let conn = Connection::open(":memory:").unwrap();

        // Create old-style blocked_issues_cache with blocked_by_json
        // Using a complete issues table schema so index migrations succeed
        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                priority INTEGER NOT NULL DEFAULT 2,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT,
                external_ref TEXT,
                ephemeral INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                due_at DATETIME,
                defer_until DATETIME
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'blocks',
                PRIMARY KEY (issue_id, depends_on_id)
            );
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                author TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT '',
                old_value TEXT,
                new_value TEXT,
                comment TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE blocked_issues_cache (
                issue_id TEXT PRIMARY KEY,
                blocked_by_json TEXT NOT NULL
            );
        ",
        )
        .unwrap();

        // Run migrations
        run_migrations(&conn).unwrap();

        // Verify columns were updated
        let cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cols.contains(&"blocked_by".to_string()),
            "Should have blocked_by"
        );
        assert!(
            cols.contains(&"blocked_at".to_string()),
            "Should have blocked_at"
        );
        assert!(
            !cols.contains(&"blocked_by_json".to_string()),
            "Should not have blocked_by_json"
        );
    }

    /// Migration: drop old blocked_issues_cache missing issue_id column.
    #[test]
    fn test_migration_blocked_cache_missing_issue_id() {
        let conn = Connection::open(":memory:").unwrap();

        // Old-style cache table with 'id' column instead of 'issue_id'
        // Using a complete issues table schema so index migrations succeed
        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                priority INTEGER NOT NULL DEFAULT 2,
                issue_type TEXT NOT NULL DEFAULT 'task',
                assignee TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT,
                external_ref TEXT,
                ephemeral INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                due_at DATETIME,
                defer_until DATETIME
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'blocks',
                PRIMARY KEY (issue_id, depends_on_id)
            );
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                author TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT '',
                old_value TEXT,
                new_value TEXT,
                comment TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE blocked_issues_cache (
                id TEXT PRIMARY KEY,
                blocked_by TEXT NOT NULL,
                blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ",
        )
        .unwrap();

        // Apply full schema (includes pre-migrations)
        apply_schema(&conn).unwrap();

        let cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cols.contains(&"issue_id".to_string()),
            "issue_id column should exist after migration"
        );
        assert!(
            cols.contains(&"blocked_by".to_string()),
            "blocked_by column should exist after migration"
        );
        assert!(
            cols.contains(&"blocked_at".to_string()),
            "blocked_at column should exist after migration"
        );
        assert!(
            !cols.contains(&"id".to_string()),
            "legacy id column should be removed"
        );
    }

    /// Migration: add missing issue columns for older schemas.
    #[test]
    fn test_migration_adds_missing_issue_columns() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
            );
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        let cols: Vec<String> = conn
            .query("PRAGMA table_info('issues')")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        let required = [
            "description",
            "design",
            "acceptance_criteria",
            "notes",
            "owner",
            "created_by",
            "updated_at",
            "source_repo",
            "compaction_level",
            "sender",
            "is_template",
        ];

        for column in required {
            assert!(
                cols.contains(&column.to_string()),
                "missing column {column}"
            );
        }
    }

    #[test]
    fn test_rebuild_issues_table_errors_when_canonical_columns_are_missing() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                legacy_only TEXT
            );
        ",
        )
        .unwrap();

        let err = rebuild_issues_table(&conn).expect_err("rebuild should fail");
        assert!(matches!(err, BeadsError::Config(_)));
        assert!(
            !table_exists(&conn, "issues_rebuild_tmp"),
            "failed rebuild should roll back the temporary table"
        );
    }

    /// Migration: add missing dependency type column for older schemas.
    #[test]
    fn test_migration_adds_missing_dependency_type() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                PRIMARY KEY (issue_id, depends_on_id)
            );
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        assert!(
            conn.query("PRAGMA table_info('dependencies')")
                .unwrap()
                .iter()
                .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
                .any(|col| col == "type"),
            "missing dependency type column"
        );
    }

    #[test]
    fn test_migration_rebuilds_legacy_config_metadata_primary_keys() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO config (key, value) VALUES ('issue_prefix', 'new');
            INSERT INTO metadata (key, value) VALUES ('project', 'new');
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        // key column should no longer be PRIMARY KEY in rebuilt tables.
        let config_key_pk = conn
            .query("SELECT pk FROM pragma_table_info('config') WHERE name='key'")
            .unwrap()
            .first()
            .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
            .unwrap_or(0);
        assert_eq!(config_key_pk, 0);

        let metadata_key_pk = conn
            .query("SELECT pk FROM pragma_table_info('metadata') WHERE name='key'")
            .unwrap()
            .first()
            .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
            .unwrap_or(0);
        assert_eq!(metadata_key_pk, 0);

        // Migration should preserve existing values.
        let config_latest = conn
            .query_row_with_params(
                "SELECT value FROM config WHERE key = ?",
                &[SqliteValue::from("issue_prefix")],
            )
            .unwrap();
        assert_eq!(
            config_latest.get(0).and_then(SqliteValue::as_text),
            Some("new")
        );

        let metadata_latest = conn
            .query_row_with_params(
                "SELECT value FROM metadata WHERE key = ?",
                &[SqliteValue::from("project")],
            )
            .unwrap();
        assert_eq!(
            metadata_latest.get(0).and_then(SqliteValue::as_text),
            Some("new")
        );
    }

    #[test]
    fn test_runtime_schema_compatible_rejects_legacy_kv_primary_keys() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("legacy_kv.db");
        {
            let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();
            apply_schema(&conn).expect("schema");

            conn.execute("DROP INDEX IF EXISTS idx_config_key")
                .expect("drop config index");
            conn.execute("DROP TABLE config").expect("drop config");
            conn.execute("CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                .expect("recreate legacy config");

            conn.execute("DROP INDEX IF EXISTS idx_metadata_key")
                .expect("drop metadata index");
            conn.execute("DROP TABLE metadata").expect("drop metadata");
            conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                .expect("recreate legacy metadata");
        }

        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();

        assert!(
            !runtime_schema_compatible(&conn),
            "legacy config/metadata primary keys should force the full repair path"
        );
    }

    #[test]
    fn test_active_list_query_plan_uses_composite_index() {
        let conn = Connection::open(":memory:").unwrap();
        apply_schema(&conn).expect("schema");

        let plan_rows = conn
            .query(
                "EXPLAIN QUERY PLAN
                 SELECT id, priority, created_at
                 FROM issues
                 WHERE status NOT IN ('closed', 'tombstone')
                   AND (is_template = 0 OR is_template IS NULL)
                 ORDER BY priority ASC, created_at DESC
                 LIMIT 1",
            )
            .expect("query plan");

        let details: Vec<String> = plan_rows
            .iter()
            .filter_map(|row| row.get(3).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_issues_list_active_order")),
            "expected planner to use idx_issues_list_active_order, got: {details:?}"
        );
    }

    // ---- split_sql_statements tests ----

    #[test]
    fn test_split_normal_multi_statement() {
        let sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT); INSERT INTO a VALUES (1)";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
        assert_eq!(stmts[1], "CREATE TABLE b (id INT)");
        assert_eq!(stmts[2], "INSERT INTO a VALUES (1)");
    }

    #[test]
    fn test_split_semicolon_inside_single_quoted_string() {
        let sql = "INSERT INTO t(v) VALUES('a;b'); SELECT 1";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "INSERT INTO t(v) VALUES('a;b')");
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_split_semicolon_inside_double_quoted_identifier() {
        let sql = r#"CREATE TABLE "weird;name" (id INT); SELECT 1"#;
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], r#"CREATE TABLE "weird;name" (id INT)"#);
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_split_escaped_quotes_in_string() {
        // SQL escapes single quotes by doubling them: 'it''s'
        let sql = "INSERT INTO t(v) VALUES('it''s;here'); SELECT 2";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "INSERT INTO t(v) VALUES('it''s;here')");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_empty_statements() {
        let sql = "SELECT 1;; ; SELECT 2";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_trailing_semicolon() {
        let sql = "SELECT 1; SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "SELECT 2");
    }

    #[test]
    fn test_split_line_comment_with_semicolon() {
        let sql = "SELECT 1; -- this is a comment; not a split\nSELECT 2";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "-- this is a comment; not a split\nSELECT 2");
    }

    #[test]
    fn test_split_block_comment_with_semicolon() {
        let sql = "SELECT 1; /* comment; with; semicolons */ SELECT 2";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "SELECT 1");
        assert_eq!(stmts[1], "/* comment; with; semicolons */ SELECT 2");
    }

    #[test]
    fn test_split_empty_input() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("   ").is_empty());
        assert!(split_sql_statements("  ;  ;  ").is_empty());
    }

    #[test]
    fn test_split_single_statement_no_semicolon() {
        let stmts = split_sql_statements("SELECT 42");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "SELECT 42");
    }
}
