use serde_json::json;
use tempfile::NamedTempFile;
use viscacha_core::JobStatus;
use viscacha_storage::PersistentSpace;

// ── event log: append and reload ─────────────────────────────────────────────

#[test]
fn append_and_reload_events() {
    use viscacha_storage::sqlite::SqliteLog;
    use viscacha_storage::event::EventOp;

    let log = SqliteLog::open_in_memory().unwrap();
    let seq1 = log.append(1000.0, &EventOp::Enqueue {
        job_id:      "job-1".into(),
        job_type:    "ocr".into(),
        args:        json!({"file": "a.pdf"}),
        max_retries: 3,
    }).unwrap();
    let seq2 = log.append(1001.0, &EventOp::Cancel { job_id: "job-1".into() }).unwrap();

    assert!(seq2 > seq1);

    let events = log.load_since(0).unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].op, EventOp::Enqueue { .. }));
    assert!(matches!(events[1].op, EventOp::Cancel { .. }));
}

#[test]
fn load_since_filters_by_seq() {
    use viscacha_storage::sqlite::SqliteLog;
    use viscacha_storage::event::EventOp;

    let log = SqliteLog::open_in_memory().unwrap();
    for i in 0..5 {
        log.append(1000.0 + i as f64, &EventOp::Cancel { job_id: format!("job-{i}") }).unwrap();
    }
    // Load only events after seq 3
    let events = log.load_since(3).unwrap();
    assert_eq!(events.len(), 2); // seq 4 and 5
}

// ── snapshot: save and restore ────────────────────────────────────────────────

#[test]
fn snapshot_roundtrip() {
    use viscacha_storage::sqlite::SqliteLog;
    use viscacha_storage::event::EventOp;

    let log = SqliteLog::open_in_memory().unwrap();
    let seq = log.append(1000.0, &EventOp::Enqueue {
        job_id: "j1".into(), job_type: "ocr".into(), args: json!({}), max_retries: 3,
    }).unwrap();

    // Manually build a job to snapshot (in real use PersistentSpace does this)
    let space = PersistentSpace::open_in_memory().unwrap();
    let id = space.enqueue("ocr".into(), json!({"n": 1}), 3).unwrap();
    let jobs = space.list(None);

    log.save_snapshot(seq, 1001.0, &jobs).unwrap();

    let (restored_seq, restored_jobs) = log.load_latest_snapshot().unwrap().unwrap();
    assert_eq!(restored_seq, seq);
    assert_eq!(restored_jobs.len(), 1);
    assert_eq!(restored_jobs[0].id, id);
}

#[test]
fn truncate_removes_old_events() {
    use viscacha_storage::sqlite::SqliteLog;
    use viscacha_storage::event::EventOp;

    let log = SqliteLog::open_in_memory().unwrap();
    let _s1 = log.append(1.0, &EventOp::Cancel { job_id: "a".into() }).unwrap();
    let s2  = log.append(2.0, &EventOp::Cancel { job_id: "b".into() }).unwrap();
    let s3  = log.append(3.0, &EventOp::Cancel { job_id: "c".into() }).unwrap();

    log.truncate_before(s2).unwrap();

    let remaining = log.load_since(0).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].seq, s3);
}

// ── persistent space: full workflow ──────────────────────────────────────────

#[test]
fn enqueue_and_get() {
    let space = PersistentSpace::open_in_memory().unwrap();
    let id = space.enqueue("send_email".into(), json!({"to": "a@b.com"}), 3).unwrap();
    let job = space.get(&id).unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.job_type, "send_email");
}

#[test]
fn claim_complete_lifecycle() {
    let space = PersistentSpace::open_in_memory().unwrap();
    space.enqueue("ocr".into(), json!({}), 3).unwrap();
    let (job, lease) = space.claim("ocr", 30.0).unwrap().unwrap();
    assert_eq!(job.status, JobStatus::Claimed);

    space.complete(&job.id, &lease, json!({"pages": 5})).unwrap();
    let done = space.get(&job.id).unwrap();
    assert_eq!(done.status, JobStatus::Done);
    assert_eq!(done.result.unwrap()["pages"], 5);
}

#[test]
fn cancel_pending_job() {
    let space = PersistentSpace::open_in_memory().unwrap();
    let id = space.enqueue("ocr".into(), json!({}), 3).unwrap();
    space.cancel(&id).unwrap();
    assert_eq!(space.get(&id).unwrap().status, JobStatus::Cancelled);
}

#[test]
fn fail_retries_then_fails_permanently() {
    let space = PersistentSpace::open_in_memory().unwrap();
    space.enqueue("ocr".into(), json!({}), 2).unwrap(); // max_retries = 2

    // First failure — requeued
    let (j1, l1) = space.claim("ocr", 30.0).unwrap().unwrap();
    space.fail(&j1.id, &l1, "timeout".into()).unwrap();
    assert_eq!(space.get(&j1.id).unwrap().status, JobStatus::Pending);

    // Second failure — permanently failed
    let (j2, l2) = space.claim("ocr", 30.0).unwrap().unwrap();
    space.fail(&j2.id, &l2, "still broken".into()).unwrap();
    assert_eq!(space.get(&j2.id).unwrap().status, JobStatus::Failed);
}

// ── crash recovery ───────────────────────────────────────────────────────────

#[test]
fn crash_recovery_via_replay() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();

    let job_id = {
        let space = PersistentSpace::open(&path).unwrap();
        let id = space.enqueue("ocr".into(), json!({"doc": "x.pdf"}), 3).unwrap();
        let (job, lease) = space.claim("ocr", 30.0).unwrap().unwrap();
        space.complete(&job.id, &lease, json!({"text": "hello"})).unwrap();
        // Simulate crash — space is dropped without explicit shutdown
        id
    };

    // Reopen — state must be fully restored from event log
    let recovered = PersistentSpace::open(&path).unwrap();
    let job = recovered.get(&job_id).unwrap();
    assert_eq!(job.status, JobStatus::Done);
    assert_eq!(job.result.unwrap()["text"], "hello");
}

#[test]
fn crash_recovery_with_snapshot() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();

    let (id1, id2) = {
        let space = PersistentSpace::open(&path).unwrap();
        let id1 = space.enqueue("ocr".into(), json!({}), 3).unwrap();
        let (j, l) = space.claim("ocr", 30.0).unwrap().unwrap();
        space.complete(&j.id, &l, json!(null)).unwrap();

        // Snapshot — events up to here are truncated
        space.snapshot().unwrap();

        // One more job after the snapshot
        let id2 = space.enqueue("ocr".into(), json!({}), 3).unwrap();
        (id1, id2)
    };

    // Recovery must reconstruct both: id1 from snapshot, id2 from post-snapshot events
    let recovered = PersistentSpace::open(&path).unwrap();
    assert_eq!(recovered.get(&id1).unwrap().status, JobStatus::Done);
    assert_eq!(recovered.get(&id2).unwrap().status, JobStatus::Pending);
}

#[test]
fn expired_lease_is_replayed_correctly() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();

    let job_id = {
        let space = PersistentSpace::open(&path).unwrap();
        let id = space.enqueue("ocr".into(), json!({}), 3).unwrap();
        // TTL of -1.0 puts lease_until in the past so expire_leases fires immediately
        let (_job, _lease) = space.claim("ocr", -1.0).unwrap().unwrap();
        space.expire_leases();
        id
    };

    let recovered = PersistentSpace::open(&path).unwrap();
    // Job must be back to Pending after replay of the Expire event
    assert_eq!(recovered.get(&job_id).unwrap().status, JobStatus::Pending);
}
