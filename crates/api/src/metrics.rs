use std::fmt::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::Response;
use viscacha_core::JobStatus;
use viscacha_storage::PersistentSpace;

const BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

pub async fn prometheus_metrics(State(space): State<Arc<PersistentSpace>>) -> Response {
    let now = unix_now();
    let all = space.list(None);

    // ── aggregate counts ────────────────────────────────────────────────────

    let pending   = all.iter().filter(|j| j.status == JobStatus::Pending).count();
    let claimed   = all.iter().filter(|j| j.status == JobStatus::Claimed).count();
    let done      = all.iter().filter(|j| j.status == JobStatus::Done).count();
    let failed    = all.iter().filter(|j| j.status == JobStatus::Failed).count();
    let cancelled = all.iter().filter(|j| j.status == JobStatus::Cancelled).count();
    let retried   = all.iter().filter(|j| j.retries > 0).count();

    let wait_secs: Vec<f64> = all.iter()
        .filter_map(|j| j.started_at.map(|s| (s - j.enqueued_at).max(0.0)))
        .collect();

    let exec_secs: Vec<f64> = all.iter()
        .filter_map(|j| j.finished_at.zip(j.started_at).map(|(f, s)| (f - s).max(0.0)))
        .collect();

    // ── per-worker counts ────────────────────────────────────────────────────

    // Collect unique worker IDs seen across all jobs that have one.
    let mut worker_claimed:   std::collections::HashMap<&str, usize> = Default::default();
    let mut worker_done:      std::collections::HashMap<&str, usize> = Default::default();
    let mut worker_failed:    std::collections::HashMap<&str, usize> = Default::default();

    for job in &all {
        if let Some(wid) = job.worker_id.as_deref() {
            match job.status {
                JobStatus::Claimed   => *worker_claimed.entry(wid).or_default() += 1,
                JobStatus::Done      => *worker_done.entry(wid).or_default()    += 1,
                JobStatus::Failed    => *worker_failed.entry(wid).or_default()  += 1,
                _ => {}
            }
        }
    }

    let mut out = String::new();

    gauge_vec(&mut out,
        "viscacha_jobs",
        "Current number of jobs in each status",
        "status",
        &[
            ("pending",   pending   as f64),
            ("claimed",   claimed   as f64),
            ("done",      done      as f64),
            ("failed",    failed    as f64),
            ("cancelled", cancelled as f64),
        ],
    );

    gauge(&mut out,
        "viscacha_retried_jobs",
        "Jobs that have been retried at least once",
        retried as f64,
    );

    histogram(&mut out,
        "viscacha_queue_wait_seconds",
        "Seconds a job spent waiting in queue before being claimed",
        &wait_secs,
    );

    histogram(&mut out,
        "viscacha_exec_seconds",
        "Seconds from claim to completion or failure",
        &exec_secs,
    );

    // ── per-worker metrics ────────────────────────────────────────────────────

    let all_workers: std::collections::HashSet<&str> = worker_claimed.keys()
        .chain(worker_done.keys())
        .chain(worker_failed.keys())
        .copied()
        .collect();

    if !all_workers.is_empty() {
        let _ = writeln!(out, "# HELP viscacha_worker_jobs Jobs attributed to each worker by status");
        let _ = writeln!(out, "# TYPE viscacha_worker_jobs gauge");
        let mut sorted: Vec<&str> = all_workers.into_iter().collect();
        sorted.sort_unstable();
        for wid in &sorted {
            let c = worker_claimed.get(wid).copied().unwrap_or(0);
            let d = worker_done.get(wid).copied().unwrap_or(0);
            let f = worker_failed.get(wid).copied().unwrap_or(0);
            let _ = writeln!(out, r#"viscacha_worker_jobs{{worker_id="{wid}",status="claimed"}} {c}"#);
            let _ = writeln!(out, r#"viscacha_worker_jobs{{worker_id="{wid}",status="done"}} {d}"#);
            let _ = writeln!(out, r#"viscacha_worker_jobs{{worker_id="{wid}",status="failed"}} {f}"#);
        }
        let _ = writeln!(out);
    }

    // ── per-job span metrics for Gantt chart ─────────────────────────────────
    //
    // High-cardinality (one series per job_id). Bounded by the in-memory job
    // set; run PersistentSpace::snapshot() to compact and reduce series count.

    let started: Vec<_> = all.iter().filter(|j| j.started_at.is_some()).collect();
    if !started.is_empty() {
        let _ = writeln!(out, "# HELP viscacha_job_start_seconds Unix timestamp when a job was claimed (ms * 1000 for Grafana Gantt)");
        let _ = writeln!(out, "# TYPE viscacha_job_start_seconds gauge");
        for job in &started {
            let wid = job.worker_id.as_deref().unwrap_or("");
            let t   = job.started_at.unwrap();
            let _ = writeln!(out,
                r#"viscacha_job_start_seconds{{job_id="{}",job_type="{}",worker_id="{wid}"}} {t}"#,
                job.id.0, job.job_type,
            );
        }
        let _ = writeln!(out);

        let _ = writeln!(out, "# HELP viscacha_job_end_seconds Unix timestamp when a job finished (finished_at, or now if still running)");
        let _ = writeln!(out, "# TYPE viscacha_job_end_seconds gauge");
        for job in &started {
            let wid    = job.worker_id.as_deref().unwrap_or("");
            let status = job.status.to_string();
            let t      = job.finished_at.unwrap_or(now);
            let _ = writeln!(out,
                r#"viscacha_job_end_seconds{{job_id="{}",job_type="{}",worker_id="{wid}",status="{status}"}} {t}"#,
                job.id.0, job.job_type,
            );
        }
        let _ = writeln!(out);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .body(Body::from(out))
        .unwrap()
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn gauge(out: &mut String, name: &str, help: &str, value: f64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
    let _ = writeln!(out);
}

fn gauge_vec(out: &mut String, name: &str, help: &str, label: &str, rows: &[(&str, f64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    for (lv, value) in rows {
        let _ = writeln!(out, r#"{name}{{{label}="{lv}"}} {value}"#);
    }
    let _ = writeln!(out);
}

fn histogram(out: &mut String, name: &str, help: &str, values: &[f64]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");

    let count = values.len() as u64;
    let sum: f64 = values.iter().copied().sum();

    for &le in BUCKETS {
        let n = values.iter().filter(|&&v| v <= le).count() as u64;
        let _ = writeln!(out, r#"{name}_bucket{{le="{le}"}} {n}"#);
    }
    let _ = writeln!(out, r#"{name}_bucket{{le="+Inf"}} {count}"#);
    let _ = writeln!(out, "{name}_sum {sum}");
    let _ = writeln!(out, "{name}_count {count}");
    let _ = writeln!(out);
}
