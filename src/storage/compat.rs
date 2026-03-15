//! Compatibility shim: provides fsqlite's Connection/Row/SqliteValue/Error API
//! backed by rusqlite.
//!
//! This eliminates fsqlite's unreliable MVCC layer while keeping all call sites
//! in the rest of the codebase unchanged.

use rusqlite;
use std::fmt;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// SqliteValue
// ---------------------------------------------------------------------------

/// Drop-in replacement for `fsqlite_types::SqliteValue`.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(v) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_real(&self) -> Option<f64> {
        match self {
            Self::Real(v) => Some(*v),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(b) => Some(b.as_slice()),
            _ => None,
        }
    }
}

impl From<&str> for SqliteValue {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl From<String> for SqliteValue {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<i64> for SqliteValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<i32> for SqliteValue {
    fn from(v: i32) -> Self {
        Self::Integer(i64::from(v))
    }
}

impl From<f64> for SqliteValue {
    fn from(v: f64) -> Self {
        Self::Real(v)
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(v: Vec<u8>) -> Self {
        Self::Blob(v)
    }
}

impl From<&[u8]> for SqliteValue {
    fn from(v: &[u8]) -> Self {
        Self::Blob(v.to_vec())
    }
}

impl rusqlite::types::ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ToSqlOutput;
        use rusqlite::types::Value;
        match self {
            Self::Null => Ok(ToSqlOutput::Owned(Value::Null)),
            Self::Integer(v) => Ok(ToSqlOutput::Owned(Value::Integer(*v))),
            Self::Real(v) => Ok(ToSqlOutput::Owned(Value::Real(*v))),
            Self::Text(s) => Ok(ToSqlOutput::Owned(Value::Text(s.clone()))),
            Self::Blob(b) => Ok(ToSqlOutput::Owned(Value::Blob(b.clone()))),
        }
    }
}

/// Provide `fsqlite_types::value::SqliteValue` re-export path.
pub mod value {
    pub use super::SqliteValue;
}

// ---------------------------------------------------------------------------
// Row
// ---------------------------------------------------------------------------

/// Drop-in replacement for `fsqlite::Row`.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<SqliteValue>,
}

impl Row {
    fn from_rusqlite_row(row: &rusqlite::Row<'_>, col_count: usize) -> rusqlite::Result<Self> {
        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let val: rusqlite::types::Value = row.get(i)?;
            columns.push(value_to_sqlite_value(val));
        }
        Ok(Self { columns })
    }

    #[must_use]
    pub fn get(&self, idx: usize) -> Option<&SqliteValue> {
        self.columns.get(idx)
    }

    #[must_use]
    pub fn values(&self) -> &[SqliteValue] {
        &self.columns
    }
}

fn value_to_sqlite_value(v: rusqlite::types::Value) -> SqliteValue {
    match v {
        rusqlite::types::Value::Null => SqliteValue::Null,
        rusqlite::types::Value::Integer(i) => SqliteValue::Integer(i),
        rusqlite::types::Value::Real(f) => SqliteValue::Real(f),
        rusqlite::types::Value::Text(s) => SqliteValue::Text(s),
        rusqlite::types::Value::Blob(b) => SqliteValue::Blob(b),
    }
}

// ---------------------------------------------------------------------------
// CompatError  (drop-in for FrankenError)
// ---------------------------------------------------------------------------

/// Drop-in replacement for `fsqlite_error::FrankenError`.
#[derive(Debug)]
pub enum CompatError {
    QueryReturnedNoRows,
    QueryReturnedMultipleRows,
    Internal(String),
    DatabaseCorrupt { detail: String },
    NotADatabase { path: PathBuf },
    WalCorrupt { detail: String },
    ShortRead { expected: usize, actual: usize },
    TableExists { name: String },
    IndexExists { name: String },
    SchemaChanged,
    CannotOpen { path: PathBuf },
    Busy,
    /// Catch-all for rusqlite errors.
    Rusqlite(rusqlite::Error),
}

impl CompatError {
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Busy => true,
            Self::Rusqlite(rusqlite::Error::SqliteFailure(e, _)) => {
                e.code == rusqlite::ffi::ErrorCode::DatabaseBusy
                    || e.code == rusqlite::ffi::ErrorCode::DatabaseLocked
            }
            _ => false,
        }
    }
}

impl fmt::Display for CompatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryReturnedNoRows => write!(f, "query returned no rows"),
            Self::QueryReturnedMultipleRows => write!(f, "query returned multiple rows"),
            Self::Internal(detail) => write!(f, "{detail}"),
            Self::DatabaseCorrupt { detail } => write!(f, "database corrupt: {detail}"),
            Self::NotADatabase { path } => write!(f, "not a database: {}", path.display()),
            Self::WalCorrupt { detail } => write!(f, "WAL corrupt: {detail}"),
            Self::ShortRead { expected, actual } => {
                write!(f, "short read: expected {expected}, got {actual}")
            }
            Self::TableExists { name } => write!(f, "table already exists: {name}"),
            Self::IndexExists { name } => write!(f, "index already exists: {name}"),
            Self::SchemaChanged => write!(f, "schema changed"),
            Self::CannotOpen { path } => write!(f, "cannot open: {}", path.display()),
            Self::Busy => write!(f, "database is busy"),
            Self::Rusqlite(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CompatError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rusqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for CompatError {
    fn from(e: rusqlite::Error) -> Self {
        match &e {
            rusqlite::Error::QueryReturnedNoRows => Self::QueryReturnedNoRows,
            rusqlite::Error::SqliteFailure(err, msg) => {
                let detail = msg.as_deref().unwrap_or("").to_string();
                match err.code {
                    rusqlite::ffi::ErrorCode::DatabaseCorrupt => {
                        Self::DatabaseCorrupt { detail }
                    }
                    rusqlite::ffi::ErrorCode::NotADatabase => Self::NotADatabase {
                        path: PathBuf::from(detail),
                    },
                    rusqlite::ffi::ErrorCode::DatabaseBusy => Self::Busy,
                    rusqlite::ffi::ErrorCode::DatabaseLocked => Self::Busy,
                    _ => {
                        if detail.is_empty() {
                            Self::Rusqlite(e)
                        } else {
                            Self::Internal(detail)
                        }
                    }
                }
            }
            _ => Self::Rusqlite(e),
        }
    }
}

// ---------------------------------------------------------------------------
// Statement
// ---------------------------------------------------------------------------

/// Wrapper that re-prepares on each query call to avoid the `&self` vs
/// `&mut self` mismatch between fsqlite and rusqlite APIs.
pub struct Statement<'conn> {
    conn: &'conn rusqlite::Connection,
    sql: String,
    col_count: usize,
}

impl<'conn> Statement<'conn> {
    pub fn query(&self) -> Result<Vec<Row>, CompatError> {
        self.query_with_params(&[])
    }

    pub fn query_with_params(&self, params: &[SqliteValue]) -> Result<Vec<Row>, CompatError> {
        let mut stmt = self.conn.prepare(&self.sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();

        let col_count = self.col_count;
        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice())?;
        while let Some(row) = rows.next()? {
            rows_out.push(Row::from_rusqlite_row(row, col_count)?);
        }
        Ok(rows_out)
    }

    /// Return EXPLAIN output as a string (diagnostic only).
    #[must_use]
    pub fn explain(&self) -> String {
        String::new()
    }
}

impl fmt::Debug for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Statement")
            .field("sql", &self.sql)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Drop-in replacement for `fsqlite::Connection` backed by rusqlite.
///
/// WAL mode and busy_timeout are NOT set here; the caller (schema.rs,
/// `open_with_timeout`) is responsible for configuring pragmas, exactly
/// as the original fsqlite code did.
pub struct Connection {
    inner: Option<rusqlite::Connection>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("open", &self.inner.is_some())
            .finish()
    }
}

impl Connection {
    /// Open a database connection.
    pub fn open<S: Into<String>>(path: S) -> Result<Self, CompatError> {
        let path_str = path.into();
        let conn = if path_str == ":memory:" {
            rusqlite::Connection::open_in_memory().map_err(CompatError::from)?
        } else {
            rusqlite::Connection::open(&path_str).map_err(CompatError::from)?
        };

        Ok(Self { inner: Some(conn) })
    }

    fn conn(&self) -> &rusqlite::Connection {
        self.inner.as_ref().expect("Connection used after close")
    }

    /// Execute SQL with no parameters. Returns rows changed.
    pub fn execute(&self, sql: &str) -> Result<usize, CompatError> {
        let conn = self.conn();
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(0);
        }

        match conn.execute(trimmed, []) {
            Ok(n) => Ok(n),
            Err(rusqlite::Error::ExecuteReturnedResults) => {
                conn.execute_batch(trimmed).map_err(CompatError::from)?;
                Ok(0)
            }
            Err(e) => Err(CompatError::from(e)),
        }
    }

    /// Execute SQL with parameters. Returns rows changed.
    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<usize, CompatError> {
        let conn = self.conn();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();
        Ok(conn.execute(sql, param_refs.as_slice())?)
    }

    /// Query with no parameters.
    pub fn query(&self, sql: &str) -> Result<Vec<Row>, CompatError> {
        self.query_with_params(sql, &[])
    }

    /// Query with parameters.
    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Vec<Row>, CompatError> {
        let conn = self.conn();
        let mut stmt = conn.prepare(sql)?;
        let col_count = stmt.column_count();
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
            .iter()
            .map(|v| v as &dyn rusqlite::types::ToSql)
            .collect();

        let mut rows_out = Vec::new();
        let mut rows = stmt.query(param_refs.as_slice())?;
        while let Some(row) = rows.next()? {
            rows_out.push(Row::from_rusqlite_row(row, col_count)?);
        }
        Ok(rows_out)
    }

    /// Query expecting exactly one row.
    pub fn query_row(&self, sql: &str) -> Result<Row, CompatError> {
        self.query_row_with_params(sql, &[])
    }

    /// Query with params expecting exactly one row.
    pub fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> Result<Row, CompatError> {
        let rows = self.query_with_params(sql, params)?;
        if rows.is_empty() {
            return Err(CompatError::QueryReturnedNoRows);
        }
        Ok(rows.into_iter().next().expect("checked non-empty"))
    }

    /// Prepare a statement for repeated execution.
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>, CompatError> {
        let conn = self.conn();
        let stmt = conn.prepare(sql)?;
        let col_count = stmt.column_count();
        drop(stmt);
        Ok(Statement {
            conn,
            sql: sql.to_string(),
            col_count,
        })
    }

    /// Close the connection, consuming self.
    pub fn close(mut self) -> Result<(), CompatError> {
        if let Some(conn) = self.inner.take() {
            conn.close().map_err(|(_conn, e)| CompatError::from(e))?;
        }
        Ok(())
    }

    /// Close the connection in place (for use in Drop).
    pub fn close_in_place(&mut self) -> Result<(), CompatError> {
        if let Some(conn) = self.inner.take() {
            conn.close().map_err(|(_conn, e)| CompatError::from(e))?;
        }
        Ok(())
    }
}
