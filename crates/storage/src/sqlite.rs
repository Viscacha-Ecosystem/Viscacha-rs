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
        let mut events = Vec::new();
        for row in stmt.query_map(params![after_seq], row_to_raw)? {
            let (seq, ts, payload) = row?;
            events.push(parse_event(seq, ts, payload)?);
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
        Ok(())
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

    /// Latest snapshot at or before the given timestamp — used by state_at().
    pub fn load_snapshot_before(&self, at: f64) -> Result<Option<(i64, Vec<Job>)>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT seq, data FROM snapshots WHERE timestamp <= ?1 ORDER BY id DESC LIMIT 1",
            params![at],
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

    /// Events with seq > after_seq and timestamp <= until_ts, in order.
    pub fn load_range(&self, after_seq: i64, until_ts: f64) -> Result<Vec<StorageEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, timestamp, payload FROM events \
             WHERE seq > ?1 AND timestamp <= ?2 ORDER BY seq ASC",
        )?;
        let mut events = Vec::new();
        for row in stmt.query_map(params![after_seq, until_ts], row_to_raw)? {
            let (seq, ts, payload) = row?;
            events.push(parse_event(seq, ts, payload)?);
        }
        Ok(events)
    }

    /// All events that touched a specific job, in order.
    pub fn load_events_for_job(&self, job_id: &str) -> Result<Vec<StorageEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, timestamp, payload FROM events \
             WHERE json_extract(payload, '$.job_id') = ?1 ORDER BY seq ASC",
        )?;
        let mut events = Vec::new();
        for row in stmt.query_map(params![job_id], row_to_raw)? {
            let (seq, ts, payload) = row?;
            events.push(parse_event(seq, ts, payload)?);
        }
        Ok(events)
    }

    /// Events within a timestamp window, up to `limit` rows.
    pub fn load_events_in_range(&self, from: f64, to: f64, limit: i64) -> Result<Vec<StorageEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, timestamp, payload FROM events \
             WHERE timestamp >= ?1 AND timestamp <= ?2 ORDER BY seq ASC LIMIT ?3",
        )?;
        let mut events = Vec::new();
        for row in stmt.query_map(params![from, to, limit], row_to_raw)? {
            let (seq, ts, payload) = row?;
            events.push(parse_event(seq, ts, payload)?);
        }
        Ok(events)
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

type RawRow = (i64, f64, String);

fn row_to_raw(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

fn parse_event(seq: i64, timestamp: f64, payload: String) -> Result<StorageEvent> {
    let op: EventOp = serde_json::from_str(&payload)?;
    Ok(StorageEvent { seq, timestamp, op })
}
