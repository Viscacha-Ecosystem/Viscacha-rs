use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, params};
use viscacha_core::Job;

use crate::error::Result;
use crate::event::{EventOp, StorageEvent};

pub struct SqliteLog {
    conn: Mutex<Connection>,
}

impl SqliteLog {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let log = SqliteLog { conn: Mutex::new(conn) };
        log.init()?;
        Ok(log)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let log = SqliteLog { conn: Mutex::new(conn) };
        log.init()?;
        Ok(log)
    }

    fn init(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous  = NORMAL;

            CREATE TABLE IF NOT EXISTS events (
                seq       INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL    NOT NULL,
                payload   TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                seq       INTEGER NOT NULL,
                timestamp REAL    NOT NULL,
                data      TEXT    NOT NULL
            );
        ")?;
        Ok(())
    }

    /// Append one event. Returns the assigned sequence number.
    pub fn append(&self, timestamp: f64, op: &EventOp) -> Result<i64> {
        let payload = serde_json::to_string(op)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO events (timestamp, payload) VALUES (?1, ?2)",
            params![timestamp, payload],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Load all events with seq > after_seq (pass 0 to get all).
    pub fn load_since(&self, after_seq: i64) -> Result<Vec<StorageEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, timestamp, payload FROM events WHERE seq > ?1 ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![after_seq], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut events = Vec::new();
        for row in rows {
            let (seq, timestamp, payload) = row?;
            let op: EventOp = serde_json::from_str(&payload)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            events.push(StorageEvent { seq, timestamp, op });
        }
        Ok(events)
    }

    /// Persist a snapshot of all current jobs. Return the snapshot id.
    pub fn save_snapshot(&self, seq: i64, timestamp: f64, jobs: &[Job]) -> Result<()> {
        let data = serde_json::to_string(jobs)?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO snapshots (seq, timestamp, data) VALUES (?1, ?2, ?3)",
            params![seq, timestamp, data],
        )?;
        Ok(()) ///sleepy guy :(
    }

    /// Load the most recent snapshot. Returns (event_seq, jobs) or None if no snapshot exists.
    pub fn load_latest_snapshot(&self) -> Result<Option<(i64, Vec<Job>)>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT seq, data FROM snapshots ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        );

        match result {
            Ok((seq, data)) => {
                let jobs: Vec<Job> = serde_json::from_str(&data)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                Ok(Some((seq, jobs)))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete events with seq <= up_to (called after a snapshot is saved).
    pub fn truncate_before(&self, up_to_seq: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM events WHERE seq <= ?1", params![up_to_seq])?;
        Ok(())
    }

    /// Current highest seq in the events table (0 if its empty).
    pub fn max_seq(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let seq: i64 = conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        Ok(seq)
    }
}
