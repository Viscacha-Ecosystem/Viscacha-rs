use viscacha_core::{Job, JobId, LeaseId, TupleSpace};

use crate::event::{EventOp, StorageEvent};

/// Rebuild a TupleSpace from an optional snapshot plus a tail of events.
/// This is called on startup: load the latest snapshot (if any), then replay
/// all events that were appended after it. The result is identical to the
/// state the space was in when it last wrote

pub fn replay(snapshot_jobs: Option<Vec<Job>>, events: Vec<StorageEvent>) -> TupleSpace {
    let space = TupleSpace::new();

    // Seed from snapshot first
    if let Some(jobs) = snapshot_jobs {
        for job in jobs {
            space.restore(job);
        }
    }

    // Apply each event in sequence order
    for ev in events {
        apply(&space, ev.op, ev.timestamp);
    }

    space
}

fn apply(space: &TupleSpace, op: EventOp, timestamp: f64) {
    match op {
        EventOp::Enqueue { job_id, job_type, args, max_retries } => {
            space.enqueue_with_id(JobId(job_id), job_type, args, max_retries, timestamp);
        }
        EventOp::Claim { job_id, lease_id, lease_until } => {
            space.apply_claim(JobId(job_id), LeaseId(lease_id), lease_until, timestamp);
        }
        EventOp::Complete { job_id, lease_id, result } => {
            let _ = space.complete(&JobId(job_id), &LeaseId(lease_id), result, timestamp);
        }
        EventOp::Fail { job_id, lease_id, error } => {
            let _ = space.fail(&JobId(job_id), &LeaseId(lease_id), error, timestamp);
        }
        EventOp::Cancel { job_id } => {
            let _ = space.cancel(&JobId(job_id), timestamp);
        }
        EventOp::Expire { job_id } => {
            space.apply_expire(JobId(job_id));
        }
    }
}

// Replay needs a few extra methods on TupleSpace that let it set specific IDs
// and apply pre-computed state rather than generating new ones.
// These live in core as `pub(crate)` 
