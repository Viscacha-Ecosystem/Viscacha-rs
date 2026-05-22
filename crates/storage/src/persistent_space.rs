use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use viscacha_core::{Job, JobId, JobStatus, LeaseId, SpaceError, TupleSpace};

use crate::error::Result;
use crate::event::{EventOp, StorageEvent};
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

    pub fn claim(&self, job_type: &str, lease_ttl_secs: f64, worker_id: Option<String>) -> Result<Option<(Job, LeaseId)>> {
        let now = unix_now();
        let Some((job, lease_id)) = self.space.claim(job_type, lease_ttl_secs, worker_id.clone(), now) else {
            return Ok(None);
        };
        let op = EventOp::Claim {
            job_id:      job.id.0.clone(),
            lease_id:    lease_id.0.clone(),
            lease_until: now + lease_ttl_secs,
            worker_id,
        };
        self.log.append(now, &op)?;
        Ok(Some((job, lease_id)))
    }

    pub fn complete(&self, job_id: &JobId, lease_id: &LeaseId, result: serde_json::Value) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
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
        self.log.append(now, &op).map_err(|e| SpaceError::Io(e.to_string()))?;
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
        self.log.append(now, &op).map_err(|e| SpaceError::Io(e.to_string()))?;
        self.space.fail(job_id, lease_id, error, now)
    }

    pub fn cancel(&self, job_id: &JobId) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        {
            let job = self.space.get(job_id).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
            if job.status != JobStatus::Pending {
                return Err(SpaceError::NotPending(job_id.clone()));
            }
        }
        let op = EventOp::Cancel { job_id: job_id.0.clone() };
        self.log.append(now, &op).map_err(|e| SpaceError::Io(e.to_string()))?;
        self.space.cancel(job_id, now)
    }

    /// Release all claimed jobs back to Pending — used on graceful shutdown.
    /// Logs each as an Expire event (best-effort) so the state survives a crash
    /// between the drain and the post-shutdown snapshot.
    pub fn drain_claimed(&self) -> Result<usize> {
        let now = unix_now();
        let claimed: Vec<JobId> = self.space
            .list(Some(JobStatus::Claimed))
            .into_iter()
            .map(|j| j.id)
            .collect();

        for id in &claimed {
            let op = EventOp::Expire { job_id: id.0.clone() };
            let _ = self.log.append(now, &op);
        }

        self.space.expire_leases(f64::MAX);
        Ok(claimed.len())
    }

    /// Reset a Failed job to Pending with zeroed retries so it can be re-processed.
    pub fn retry(&self, job_id: &JobId) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        {
            let job = self.space.get(job_id).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
            if job.status != JobStatus::Failed {
                return Err(SpaceError::InvalidTransition {
                    from: job.status.to_string(),
                    to: "pending".into(),
                });
            }
        }
        let op = EventOp::Retry { job_id: job_id.0.clone() };
        self.log.append(now, &op).map_err(|e| SpaceError::Io(e.to_string()))?;
        self.space.retry(job_id, now)
    }

    /// Extend the lease on a Claimed job without writing to the log.
    /// Heartbeats are ephemeral — on restart, the reaper handles any still-claimed jobs.
    pub fn extend_lease(&self, job_id: &JobId, lease_id: &LeaseId, extend_by: f64) -> std::result::Result<(), SpaceError> {
        let now = unix_now();
        self.space.extend_lease(job_id, lease_id, now + extend_by)
    }

    /// Remove terminal jobs (Done/Failed/Cancelled) whose finished_at is older than
    /// `older_than_secs` seconds ago. Returns the number of jobs purged.
    pub fn purge_terminal(&self, older_than_secs: f64) -> Result<usize> {
        let now = unix_now();
        let cutoff = now - older_than_secs;
        let to_purge: Vec<JobId> = self.space
            .list(None)
            .into_iter()
            .filter(|j| {
                matches!(j.status, JobStatus::Done | JobStatus::Failed | JobStatus::Cancelled)
                    && j.finished_at.map_or(false, |t| t < cutoff)
            })
            .map(|j| j.id)
            .collect();

        for id in &to_purge {
            let op = EventOp::Purge { job_id: id.0.clone() };
            self.log.append(now, &op)?;
            self.space.remove(id);
        }

        Ok(to_purge.len())
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

    // replay / time-travel

    /// Reconstruct the full job list as it was at the given Unix timestamp.
    /// Accuracy is limited by snapshot retention: only events after the most
    /// recent snapshot before `at` are guaranteed to be available.
    pub fn state_at(&self, at: f64) -> Result<Vec<Job>> {
        let (snap_seq, snap_jobs) = match self.log.load_snapshot_before(at)? {
            Some((seq, jobs)) => (seq, Some(jobs)),
            None              => (0,   None),
        };
        let events = self.log.load_range(snap_seq, at)?;
        Ok(replay(snap_jobs, events).list(None))
    }

    /// All events that touched a specific job, in log order.
    pub fn events_for_job(&self, job_id: &str) -> Result<Vec<StorageEvent>> {
        self.log.load_events_for_job(job_id)
    }

    /// Events within a timestamp window, newest first up to `limit`.
    pub fn events_in_range(&self, from: f64, to: f64, limit: usize) -> Result<Vec<StorageEvent>> {
        self.log.load_events_in_range(from, to, limit as i64)
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
