use std::collections::{BTreeMap, HashMap};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::error::SpaceError;
use crate::types::{Job, JobId, JobStatus, LeaseId};

struct Inner {
    jobs:    HashMap<String, Job>,
    // Sorted index of pending jobs: (job_type, enqueued_at_bits, job_id) → ()
    // enqueued_at_bits is f64::to_bits(), which preserves total order for
    // positive finite timestamps, giving us FIFO claim within each job type.
    pending: BTreeMap<(String, u64, String), ()>,
}

impl Inner {
    fn new() -> Self {
        Inner { jobs: HashMap::new(), pending: BTreeMap::new() }
    }

    fn pending_key(job: &Job) -> (String, u64, String) {
        (job.job_type.clone(), job.enqueued_at.to_bits(), job.id.0.clone())
    }

    fn add_pending(&mut self, job: &Job) {
        self.pending.insert(Self::pending_key(job), ());
    }
}

/// Thread-safe in-memory job store.
/// All methods take &self — interior mutability via RwLock.
pub struct TupleSpace {
    inner: RwLock<Inner>,
}

impl TupleSpace {
    pub fn new() -> Self {
        TupleSpace { inner: RwLock::new(Inner::new()) }
    }

    // write ops

    /// Enqueue a new job. Returns the assigned JobId.
    pub fn enqueue(&self, job_type: String, args: serde_json::Value, max_retries: u8, now: f64) -> JobId {
        let id = JobId(Uuid::new_v4().to_string());
        let job = Job::new(id.clone(), job_type, args, max_retries, now);
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        inner.add_pending(&job);
        inner.jobs.insert(id.0.clone(), job);
        id
    }

    /// Claim the oldest pending job of a given type, stamping a lease.
    /// Returns (job snapshot, lease_id) or None if no matching job is available.
    pub fn claim(&self, job_type: &str, lease_ttl_secs: f64, worker_id: Option<String>, now: f64) -> Option<(Job, LeaseId)> {
        let mut guard = self.inner.write();
        let inner = &mut *guard;

        // Range from the minimum key for this job_type — first hit is the oldest.
        let key = inner.pending
            .range((job_type.to_string(), 0_u64, String::new())..)
            .next()
            .filter(|((jt, _, _), _)| jt == job_type)
            .map(|(k, _)| k.clone())?;

        inner.pending.remove(&key);
        let job_id = &key.2;

        let job = inner.jobs.get_mut(job_id)?;
        let lease_id = LeaseId::new();
        job.status      = JobStatus::Claimed;
        job.started_at  = Some(now);
        job.lease_until = Some(now + lease_ttl_secs);
        job.lease_id    = Some(lease_id.clone());
        job.worker_id   = worker_id;

        Some((job.clone(), lease_id))
    }

    /// Mark a claimed job as done.
    pub fn complete(&self, job_id: &JobId, lease_id: &LeaseId, result: serde_json::Value, now: f64) -> Result<(), SpaceError> {
        let mut guard = self.inner.write();
        let job = guard.jobs.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        check_lease(job, lease_id)?;

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
        let inner = &mut *guard;
        let job = inner.jobs.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        check_lease(job, lease_id)?;

        job.retries    += 1;
        job.lease_until = None;
        job.lease_id    = None;

        if job.retries < job.max_retries {
            job.status     = JobStatus::Pending;
            job.started_at = None;
            let key = Inner::pending_key(job);
            inner.pending.insert(key, ());
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
        let inner = &mut *guard;
        let job = inner.jobs.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        if job.status != JobStatus::Pending {
            return Err(SpaceError::NotPending(job_id.clone()));
        }
        let key = Inner::pending_key(job);
        inner.pending.remove(&key);
        job.status      = JobStatus::Cancelled;
        job.finished_at = Some(now);
        Ok(())
    }

    /// Reset a Failed job back to Pending with zeroed retries.
    pub fn retry(&self, job_id: &JobId, now: f64) -> Result<(), SpaceError> {
        let _ = now;
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        let job = inner.jobs.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        if job.status != JobStatus::Failed {
            return Err(SpaceError::InvalidTransition {
                from: job.status.to_string(),
                to: "pending".into(),
            });
        }
        job.status      = JobStatus::Pending;
        job.error       = None;
        job.retries     = 0;
        job.finished_at = None;
        let key = Inner::pending_key(job);
        inner.pending.insert(key, ());
        Ok(())
    }

    /// Extend the lease on a Claimed job to `new_lease_until`.
    pub fn extend_lease(&self, job_id: &JobId, lease_id: &LeaseId, new_lease_until: f64) -> Result<(), SpaceError> {
        let mut guard = self.inner.write();
        let job = guard.jobs.get_mut(&job_id.0).ok_or_else(|| SpaceError::NotFound(job_id.clone()))?;
        check_lease(job, lease_id)?;
        job.lease_until = Some(new_lease_until);
        Ok(())
    }

    /// Expire all claimed jobs whose lease_until < now, returning them to Pending.
    /// Returns the number of jobs expired.
    pub fn expire_leases(&self, now: f64) -> usize {
        let mut guard = self.inner.write();
        let inner = &mut *guard;

        let expired_ids: Vec<String> = inner.jobs.values()
            .filter(|j| j.status == JobStatus::Claimed)
            .filter(|j| j.lease_until.map_or(false, |t| now > t))
            .map(|j| j.id.0.clone())
            .collect();

        for job_id in &expired_ids {
            if let Some(job) = inner.jobs.get_mut(job_id) {
                job.status      = JobStatus::Pending;
                job.started_at  = None;
                job.lease_until = None;
                job.lease_id    = None;
                let key = Inner::pending_key(job);
                inner.pending.insert(key, ());
            }
        }

        expired_ids.len()
    }

    // read ops

    pub fn get(&self, job_id: &JobId) -> Option<Job> {
        self.inner.read().jobs.get(&job_id.0).cloned()
    }

    pub fn list(&self, status: Option<JobStatus>) -> Vec<Job> {
        let guard = self.inner.read();
        guard.jobs.values()
            .filter(|j| status.map_or(true, |s| j.status == s))
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.inner.read().jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().jobs.is_empty()
    }

    // replay / restore (used by storage crate)

    /// Insert a job directly — used when seeding from a snapshot.
    pub fn restore(&self, job: Job) {
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        if job.status == JobStatus::Pending {
            inner.add_pending(&job);
        }
        inner.jobs.insert(job.id.0.clone(), job);
    }

    /// Remove a job by id — used by purge.
    pub fn remove(&self, job_id: &JobId) {
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        if let Some(job) = inner.jobs.remove(&job_id.0) {
            if job.status == JobStatus::Pending {
                inner.pending.remove(&Inner::pending_key(&job));
            }
        }
    }

    /// Enqueue with a caller-supplied ID — used during event log replay.
    pub fn enqueue_with_id(&self, id: JobId, job_type: String, args: serde_json::Value, max_retries: u8, now: f64) {
        let job = Job::new(id.clone(), job_type, args, max_retries, now);
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        inner.add_pending(&job);
        inner.jobs.insert(id.0, job);
    }

    /// Apply a pre-computed claim during replay (lease already decided).
    pub fn apply_claim(&self, job_id: JobId, lease_id: LeaseId, lease_until: f64, started_at: f64, worker_id: Option<String>) {
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        if let Some(job) = inner.jobs.get_mut(&job_id.0) {
            inner.pending.remove(&Inner::pending_key(job));
            job.status      = JobStatus::Claimed;
            job.started_at  = Some(started_at);
            job.lease_until = Some(lease_until);
            job.lease_id    = Some(lease_id);
            job.worker_id   = worker_id;
        }
    }

    /// Apply an expiry to a specific job during replay.
    pub fn apply_expire(&self, job_id: JobId) {
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        if let Some(job) = inner.jobs.get_mut(&job_id.0) {
            if job.status == JobStatus::Claimed {
                job.status      = JobStatus::Pending;
                job.started_at  = None;
                job.lease_until = None;
                job.lease_id    = None;
                let key = Inner::pending_key(job);
                inner.pending.insert(key, ());
            }
        }
    }
}

impl Default for TupleSpace {
    fn default() -> Self {
        Self::new()
    }
}

fn check_lease(job: &Job, lease_id: &LeaseId) -> Result<(), SpaceError> {
    if job.status != JobStatus::Claimed {
        return Err(SpaceError::NotClaimed(job.id.clone()));
    }
    if job.lease_id.as_ref() != Some(lease_id) {
        return Err(SpaceError::LeaseInvalid);
    }
    Ok(())
}
