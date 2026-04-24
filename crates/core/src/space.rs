use std::collections::HashMap;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::SpaceError;
use crate::types::{Job, JobId, JobStatus, LeaseId};

/// Thread-safe in-memory job store.
/// All methods take &self — interior mutability via RwLock.
pub struct TupleSpace {
    inner: RwLock<HashMap<String, Job>>,
}

impl TupleSpace {
    pub fn new() -> Self {
        TupleSpace {
            inner: RwLock::new(HashMap::new()),
        }
    }

    // write ops

    /// Enqueue a new job. Returns the assigned JobId.
    pub fn enqueue(&self, job_type: String, args: serde_json::Value, max_retries: u8, now: f64) -> JobId {
        let id = JobId(Uuid::new_v4().to_string());
        let job = Job::new(id.clone(), job_type, args, max_retries, now);
        self.inner.write().insert(id.0.clone(), job);
        id
    }

    /// Claim the next pending job of a given type, stamping a lease.
    /// Returns (job snapshot, lease_id) or None if no matching job is available.
    pub fn claim(&self, job_type: &str, lease_ttl_secs: f64, now: f64) -> Option<(Job, LeaseId)> {
        let mut guard = self.inner.write();
        let job = guard.values_mut().find(|j| {
            j.status == JobStatus::Pending && j.job_type == job_type
        })?;

        let lease_id = LeaseId::new();
        job.status      = JobStatus::Claimed;
        job.started_at  = Some(now);
        job.lease_until = Some(now + lease_ttl_secs);
        job.lease_id    = Some(lease_id.clone());

        Some((job.clone(), lease_id))
    }

    /// Mark a claimed job as done.
    pub fn complete(&self, job_id: &JobId, lease_id: &LeaseId, result: serde_json::Value, now: f64) -> Result<(), SpaceError> {
        let mut guard = self.inner.write();
        let job = guard.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        self.check_lease(job, lease_id)?;

        job.status      = JobStatus::Done;
        job.result      = Some(result);
        job.finished_at = Some(now);
        job.lease_until = None;
        job.lease_id    = None;
        Ok(())
    }

    /// Mark a claimed job as failed, re-queuing it if retries remain.
    pub fn fail(&self, job_id: &JobId, lease_id: &LeaseId, error: String, now: f64) -> Result<(), SpaceError> {
        let mut guard = self.inner.write();
        let job = guard.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        self.check_lease(job, lease_id)?;

        job.retries    += 1;
        job.lease_until = None;
        job.lease_id    = None;

        if job.retries < job.max_retries {
            job.status     = JobStatus::Pending;
            job.started_at = None;
        } else {
            job.status      = JobStatus::Failed;
            job.error       = Some(error);
            job.finished_at = Some(now);
        }
        Ok(())
    }

    /// Cancel a pending job.
    pub fn cancel(&self, job_id: &JobId, now: f64) -> Result<(), SpaceError> {
        let mut guard = self.inner.write();
        let job = guard.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        if job.status != JobStatus::Pending {
            return Err(SpaceError::NotPending(job_id.clone()));
        }
        job.status      = JobStatus::Cancelled;
        job.finished_at = Some(now);
        Ok(())
    }

    /// Expire all claimed jobs whose lease_until < now, returning them to Pending.
    /// Returns the number of jobs expired.
    pub fn expire_leases(&self, now: f64) -> usize {
        let mut guard = self.inner.write();
        let mut count = 0;
        for job in guard.values_mut() {
            if job.status == JobStatus::Claimed {
                if let Some(until) = job.lease_until {
                    if now > until {
                        job.status      = JobStatus::Pending;
                        job.started_at  = None;
                        job.lease_until = None;
                        job.lease_id    = None;
                        count += 1;
                    }
                }
            }
        }
        count
    }

    // read ops 

    pub fn get(&self, job_id: &JobId) -> Option<Job> {
        self.inner.read().get(&job_id.0).cloned()
    }

    pub fn list(&self, status: Option<JobStatus>) -> Vec<Job> {
        let guard = self.inner.read();
        guard.values()
            .filter(|j| status.map_or(true, |s| j.status == s))
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    // replay / restore (used by storage crate) 

    /// Insert a job directly — used when seeding from a snapshot.
    pub fn restore(&self, job: Job) {
        self.inner.write().insert(job.id.0.clone(), job);
    }

    /// Enqueue with a caller-supplied ID — used during event log replay.
    pub fn enqueue_with_id(&self, id: JobId, job_type: String, args: serde_json::Value, max_retries: u8, now: f64) {
        let job = Job::new(id.clone(), job_type, args, max_retries, now);
        self.inner.write().insert(id.0, job);
    }

    /// Apply a pre-computed claim during replay (lease already decided).
    pub fn apply_claim(&self, job_id: JobId, lease_id: LeaseId, lease_until: f64, started_at: f64) {
        let mut guard = self.inner.write();
        if let Some(job) = guard.get_mut(&job_id.0) {
            job.status      = JobStatus::Claimed;
            job.started_at  = Some(started_at);
            job.lease_until = Some(lease_until);
            job.lease_id    = Some(lease_id);
        }
    }

    /// Apply an expiry to a specific job during replay.
    pub fn apply_expire(&self, job_id: JobId) {
        let mut guard = self.inner.write();
        if let Some(job) = guard.get_mut(&job_id.0) {
            if job.status == JobStatus::Claimed {
                job.status      = JobStatus::Pending;
                job.started_at  = None;
                job.lease_until = None;
                job.lease_id    = None;
            }
        }
    }

    // internal 

    fn check_lease(&self, job: &Job, lease_id: &LeaseId) -> Result<(), SpaceError> {
        if job.status != JobStatus::Claimed {
            return Err(SpaceError::NotClaimed(job.id.clone()));
        }
        if job.lease_id.as_ref() != Some(lease_id) {
            return Err(SpaceError::LeaseInvalid);
        }
        Ok(())
    }
}

impl Default for TupleSpace {
    fn default() -> Self {
        Self::new()
    }
}
