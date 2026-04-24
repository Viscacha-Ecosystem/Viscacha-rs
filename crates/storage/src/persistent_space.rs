use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use viscacha_core::{Job, JobId, JobStatus, LeaseId, SpaceError, TupleSpace};

use crate::error::Result;
use crate::event::EventOp;
use crate::replay::replay;
use crate::sqlite::SqliteLog;

/// TupleSpace backed by a SQLite event log.
///
/// Every mutation is appended to the log before being applied in-memory.
/// On startup, the log (plus the latest snapshot) is replayed to restore state.
pub struct PersistentSpace {
    space: TupleSpace,
    log:   SqliteLog,
}

impl PersistentSpace {
    pub fn open(path: &Path) -> Result<Self> {
        let log = SqliteLog::open(path)?;
        let space = Self::load(&log)?;
        Ok(PersistentSpace { space, log })
    }

    pub fn open_in_memory() -> Result<Self> {
        let log = SqliteLog::open_in_memory()?;
        Ok(PersistentSpace { space: TupleSpace::new(), log })
    }

    fn load(log: &SqliteLog) -> Result<TupleSpace> {
        let (after_seq, snapshot_jobs) = match log.load_latest_snapshot()? {
            Some((seq, jobs)) => (seq, Some(jobs)),
            None              => (0,   None),
        };
        let events = log.load_since(after_seq)?;
        Ok(replay(snapshot_jobs, events))
    }

    // write ops

    pub fn enqueue(&self, job_type: String, args: serde_json::Value, max_retries: u8) -> Result<JobId> {
        let now = unix_now();
        let id = JobId(uuid::Uuid::new_v4().to_string());
        let op = EventOp::Enqueue {
            job_id:      id.0.clone(),
            job_type:    job_type.clone(),
            args:        args.clone(),
            max_retries,
        };
        self.log.append(now, &op)?;
        self.space.enqueue_with_id(id.clone(), job_type, args, max_retries, now);
        Ok(id)
    }

    pub fn claim(&self, job_type: &str, lease_ttl_secs: f64) -> Result<Option<(Job, LeaseId)>> {
        let now = unix_now();
        let Some((job, lease_id)) = self.space.claim(job_type, lease_ttl_secs, now) else {
            return Ok(None);
        };
        let op = EventOp::Claim {
            job_id:      job.id.0.clone(),
            lease_id:    lease_id.0.clone(),
            lease_until: now + lease_ttl_secs,
        };
        self.log.append(now, &op)?;
        Ok(Some((job, lease_id)))
    }

    pub fn complete(&self, job_id: &JobId, lease_id: &LeaseId, result: serde_json::Value) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        // Validate lease before writing to log
        {
            let job = self.space.get(job_id).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
            if job.lease_id.as_ref() != Some(lease_id) {
                return Err(SpaceError::LeaseInvalid);
            }
        }
        let op = EventOp::Complete {
            job_id:   job_id.0.clone(),
            lease_id: lease_id.0.clone(),
            result:   result.clone(),
        };
        // Best-effort log — if this fails the in-memory state is unchanged
        let _ = self.log.append(now, &op);
        self.space.complete(job_id, lease_id, result, now)
    }

    pub fn fail(&self, job_id: &JobId, lease_id: &LeaseId, error: String) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        {
            let job = self.space.get(job_id).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
            if job.lease_id.as_ref() != Some(lease_id) {
                return Err(SpaceError::LeaseInvalid);
            }
        }
        let op = EventOp::Fail {
            job_id:   job_id.0.clone(),
            lease_id: lease_id.0.clone(),
            error:    error.clone(),
        };
        let _ = self.log.append(now, &op);
        self.space.fail(job_id, lease_id, error, now)
    }

    pub fn cancel(&self, job_id: &JobId) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        let op = EventOp::Cancel { job_id: job_id.0.clone() };
        let _ = self.log.append(now, &op);
        self.space.cancel(job_id, now)
    }

    pub fn expire_leases(&self) -> usize {
        let now = unix_now();
        // Collect IDs of jobs that will expire before mutating
        let expiring: Vec<JobId> = self.space
            .list(Some(JobStatus::Claimed))
            .into_iter()
            .filter(|j| j.lease_until.map_or(false, |t| now > t))
            .map(|j| j.id)
            .collect();

        for id in &expiring {
            let op = EventOp::Expire { job_id: id.0.clone() };
            let _ = self.log.append(now, &op);
        }

        self.space.expire_leases(now)
    }

    /// Write a snapshot of current state and truncate old events.
    pub fn snapshot(&self) -> Result<()> {
        let now    = unix_now();
        let seq    = self.log.max_seq()?;
        let jobs   = self.space.list(None);
        self.log.save_snapshot(seq, now, &jobs)?;
        self.log.truncate_before(seq)?;
        Ok(())
    }

    // read op

    pub fn get(&self, job_id: &JobId) -> Option<Job> {
        self.space.get(job_id)
    }

    pub fn list(&self, status: Option<JobStatus>) -> Vec<Job> {
        self.space.list(status)
    }
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
