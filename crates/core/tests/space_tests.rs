use viscacha_core::{JobStatus, TupleSpace};
use serde_json::json;

fn now() -> f64 { 1_000_000.0 }
fn later(secs: f64) -> f64 { now() + secs }

// ── enqueue ───────────────────────────────────────────────────────────────────

#[test]
fn enqueue_returns_unique_ids() {
    let space = TupleSpace::new();
    let a = space.enqueue("send_email".into(), json!({}), 3, now());
    let b = space.enqueue("send_email".into(), json!({}), 3, now());
    assert_ne!(a, b);
}

#[test]
fn enqueued_job_is_pending() {
    let space = TupleSpace::new();
    let id = space.enqueue("resize_image".into(), json!({"w": 800}), 3, now());
    let job = space.get(&id).unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.job_type, "resize_image");
    assert_eq!(job.args["w"], 800);
}

// ── claim ─────────────────────────────────────────────────────────────────────

#[test]
fn claim_transitions_to_claimed() {
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, _lease) = space.claim("ocr", 30.0, now()).unwrap();
    assert_eq!(job.id, id);
    assert_eq!(job.status, JobStatus::Claimed);
    assert!(job.lease_until.is_some());
    assert!(job.started_at.is_some());
}

#[test]
fn claim_returns_none_when_queue_empty() {
    let space = TupleSpace::new();
    assert!(space.claim("ocr", 30.0, now()).is_none());
}

#[test]
fn claim_only_matches_correct_job_type() {
    let space = TupleSpace::new();
    space.enqueue("send_email".into(), json!({}), 3, now());
    assert!(space.claim("ocr", 30.0, now()).is_none());
}

// ── complete ──────────────────────────────────────────────────────────────────

#[test]
fn complete_marks_job_done() {
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, lease) = space.claim("ocr", 30.0, now()).unwrap();
    space.complete(&job.id, &lease, json!({"text": "hello"}), later(1.0)).unwrap();
    let done = space.get(&id).unwrap();
    assert_eq!(done.status, JobStatus::Done);
    assert_eq!(done.result.unwrap()["text"], "hello");
    assert!(done.finished_at.is_some());
    assert!(done.lease_until.is_none());
}

#[test]
fn complete_rejects_wrong_lease() {
    use viscacha_core::{LeaseId, SpaceError};
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, _real_lease) = space.claim("ocr", 30.0, now()).unwrap();
    let fake_lease = LeaseId::new();
    let err = space.complete(&job.id, &fake_lease, json!(null), later(1.0)).unwrap_err();
    assert!(matches!(err, SpaceError::LeaseInvalid));
    // job must still be claimed — no state change on bad lease
    assert_eq!(space.get(&id).unwrap().status, JobStatus::Claimed);
}

// ── fail / retry ──────────────────────────────────────────────────────────────

#[test]
fn fail_requeues_when_retries_remain() {
    let space = TupleSpace::new();
    space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, lease) = space.claim("ocr", 30.0, now()).unwrap();
    space.fail(&job.id, &lease, "timeout".into(), later(1.0)).unwrap();
    let requeued = space.get(&job.id).unwrap();
    assert_eq!(requeued.status, JobStatus::Pending);
    assert_eq!(requeued.retries, 1);
    assert!(requeued.lease_until.is_none());
}

#[test]
fn fail_marks_failed_when_retries_exhausted() {
    let space = TupleSpace::new();
    space.enqueue("ocr".into(), json!({}), 1, now()); // max_retries = 1
    let (job, lease) = space.claim("ocr", 30.0, now()).unwrap();
    space.fail(&job.id, &lease, "boom".into(), later(1.0)).unwrap();
    let failed = space.get(&job.id).unwrap();
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.error.as_deref(), Some("boom"));
    assert!(failed.finished_at.is_some());
}

// ── cancel ────────────────────────────────────────────────────────────────────

#[test]
fn cancel_pending_job() {
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    space.cancel(&id, later(1.0)).unwrap();
    assert_eq!(space.get(&id).unwrap().status, JobStatus::Cancelled);
}

#[test]
fn cancel_non_pending_job_errors() {
    use viscacha_core::SpaceError;
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, lease) = space.claim("ocr", 30.0, now()).unwrap();
    space.complete(&job.id, &lease, json!(null), later(1.0)).unwrap();
    let err = space.cancel(&id, later(2.0)).unwrap_err();
    assert!(matches!(err, SpaceError::NotPending(_)));
}

// ── lease expiry ──────────────────────────────────────────────────────────────

#[test]
fn expired_lease_returns_job_to_pending() {
    let space = TupleSpace::new();
    let id = space.enqueue("ocr".into(), json!({}), 3, now());
    let (_job, _lease) = space.claim("ocr", 10.0, now()).unwrap(); // expires at now+10
    let expired = space.expire_leases(later(11.0)); // advance past expiry
    assert_eq!(expired, 1);
    assert_eq!(space.get(&id).unwrap().status, JobStatus::Pending);
}

#[test]
fn non_expired_lease_not_reaped() {
    let space = TupleSpace::new();
    space.enqueue("ocr".into(), json!({}), 3, now());
    space.claim("ocr", 30.0, now()).unwrap();
    let expired = space.expire_leases(later(5.0)); // only 5s in, lease TTL is 30s
    assert_eq!(expired, 0);
}

// ── list ──────────────────────────────────────────────────────────────────────

#[test]
fn list_filters_by_status() {
    let space = TupleSpace::new();
    space.enqueue("ocr".into(), json!({}), 3, now());
    space.enqueue("ocr".into(), json!({}), 3, now());
    let (job, lease) = space.claim("ocr", 30.0, now()).unwrap();
    space.complete(&job.id, &lease, json!(null), later(1.0)).unwrap();

    assert_eq!(space.list(Some(JobStatus::Pending)).len(), 1);
    assert_eq!(space.list(Some(JobStatus::Done)).len(), 1);
    assert_eq!(space.list(None).len(), 2);
}
