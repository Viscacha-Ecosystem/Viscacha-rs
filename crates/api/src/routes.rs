use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use utoipa::IntoParams;
use viscacha_core::JobId;
use viscacha_storage::PersistentSpace;

use crate::error::ApiError;
use crate::models::{
    ClaimBody, ClaimResponse, CompleteBody, EnqueueBody, EnqueueResponse, FailBody, HeartbeatBody,
    JobView, ListResponse,
};

pub type AppState = Arc<PersistentSpace>;

/// Enqueue a new job.
#[utoipa::path(
    post, path = "/jobs",
    request_body = EnqueueBody,
    responses(
        (status = 201, description = "Job enqueued", body = EnqueueResponse),
    ),
    tag = "jobs"
)]
pub async fn enqueue_job(
    State(space): State<AppState>,
    Json(body): Json<EnqueueBody>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let job_id = tokio::task::spawn_blocking(move || {
        space.enqueue(body.job_type, body.args, body.max_retries)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok((StatusCode::CREATED, Json(EnqueueResponse { job_id: job_id.0 })))
}

#[derive(Deserialize, IntoParams)]
pub struct ListQuery {
    /// Filter by job status: pending | claimed | done | failed | cancelled
    pub status: Option<String>,
}

/// List all jobs, optionally filtered by status.
#[utoipa::path(
    get, path = "/jobs",
    params(ListQuery),
    responses(
        (status = 200, description = "List of jobs", body = ListResponse),
    ),
    tag = "jobs"
)]
pub async fn list_jobs(
    State(space): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListResponse>, ApiError> {
    let status = q.status.as_deref().map(parse_status).transpose()?;
    let jobs = tokio::task::spawn_blocking(move || space.list(status))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .into_iter()
        .map(JobView::from)
        .collect();
    Ok(Json(ListResponse { jobs }))
}

/// Get a single job by ID.
#[utoipa::path(
    get, path = "/jobs/{id}",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job found", body = JobView),
        (status = 404, description = "Job not found"),
    ),
    tag = "jobs"
)]
pub async fn get_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    let job = tokio::task::spawn_blocking(move || space.get(&JobId(id)))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::NotFound("job not found".into()))?;
    Ok(Json(JobView::from(job)))
}

/// Cancel a pending job.
#[utoipa::path(
    post, path = "/jobs/{id}/cancel",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job cancelled"),
        (status = 404, description = "Job not found or not pending"),
    ),
    tag = "jobs"
)]
pub async fn cancel_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    tokio::task::spawn_blocking(move || space.cancel(&JobId(id)).map_err(ApiError::from))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(serde_json::json!({ "status": "cancelled" })))
}

/// Re-queue a permanently-failed job.
#[utoipa::path(
    post, path = "/jobs/{id}/retry",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job re-queued as pending"),
        (status = 404, description = "Job not found or not in failed state"),
    ),
    tag = "jobs"
)]
pub async fn retry_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    tokio::task::spawn_blocking(move || space.retry(&JobId(id)).map_err(ApiError::from))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(serde_json::json!({ "status": "pending" })))
}

/// Claim the next available job of the given type.
#[utoipa::path(
    post, path = "/jobs/claim",
    request_body = ClaimBody,
    responses(
        (status = 200, description = "Job claimed", body = ClaimResponse),
        (status = 204, description = "No job available"),
    ),
    tag = "workers"
)]
pub async fn claim_job(
    State(space): State<AppState>,
    Json(body): Json<ClaimBody>,
) -> Result<impl IntoResponse, ApiError> {
    let result = tokio::task::spawn_blocking(move || {
        space.claim(&body.job_type, body.lease_ttl, body.worker_id)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    match result {
        None => Ok(StatusCode::NO_CONTENT.into_response()),
        Some((job, lease_id)) => Ok(Json(ClaimResponse {
            job:      JobView::from(job),
            lease_id: lease_id.0,
        })
        .into_response()),
    }
}

/// Mark a claimed job as done.
#[utoipa::path(
    post, path = "/jobs/{id}/complete",
    params(("id" = String, Path, description = "Job ID")),
    request_body = CompleteBody,
    responses(
        (status = 200, description = "Job marked done"),
        (status = 400, description = "Bad or expired lease"),
    ),
    tag = "workers"
)]
pub async fn complete_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<CompleteBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::LeaseId;
    tokio::task::spawn_blocking(move || {
        space.complete(&JobId(id), &LeaseId(body.lease_id), body.result)
            .map_err(ApiError::from)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(serde_json::json!({ "status": "done" })))
}

/// Mark a claimed job as failed. Retries if retries < max_retries.
#[utoipa::path(
    post, path = "/jobs/{id}/fail",
    params(("id" = String, Path, description = "Job ID")),
    request_body = FailBody,
    responses(
        (status = 200, description = "Job marked failed (or re-queued for retry)"),
        (status = 400, description = "Bad or expired lease"),
    ),
    tag = "workers"
)]
pub async fn fail_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<FailBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::LeaseId;
    tokio::task::spawn_blocking(move || {
        space.fail(&JobId(id), &LeaseId(body.lease_id), body.error)
            .map_err(ApiError::from)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// Extend a claimed job's lease.
#[utoipa::path(
    post, path = "/jobs/{id}/heartbeat",
    params(("id" = String, Path, description = "Job ID")),
    request_body = HeartbeatBody,
    responses(
        (status = 200, description = "Lease extended"),
        (status = 400, description = "Bad or expired lease"),
    ),
    tag = "workers"
)]
pub async fn heartbeat_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<HeartbeatBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::LeaseId;
    tokio::task::spawn_blocking(move || {
        space.extend_lease(&JobId(id), &LeaseId(body.lease_id), body.extend_secs)
            .map_err(ApiError::from)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;
    Ok(Json(serde_json::json!({ "status": "ok" })))
}

/// Get a structured trace timeline for a job.
#[utoipa::path(
    get, path = "/jobs/{id}/trace",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Job trace with timeline entries"),
        (status = 404, description = "Job not found"),
    ),
    tag = "observability"
)]
pub async fn trace_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::JobStatus;
    let job = tokio::task::spawn_blocking(move || space.get(&JobId(id)))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))?
        .ok_or_else(|| ApiError::NotFound("job not found".into()))?;

    let t0 = job.enqueued_at;
    let mut timeline = vec![serde_json::json!({
        "event":     "enqueued",
        "at":        t0,
        "offset_ms": 0
    })];

    if let Some(started_at) = job.started_at {
        timeline.push(serde_json::json!({
            "event":     "claimed",
            "at":        started_at,
            "offset_ms": ((started_at - t0) * 1000.0) as i64,
            "worker_id": job.worker_id
        }));
    }

    if let Some(finished_at) = job.finished_at {
        let event = match job.status {
            JobStatus::Done      => "done",
            JobStatus::Failed    => "failed",
            JobStatus::Cancelled => "cancelled",
            _                    => "finished",
        };
        let exec_ms = job.started_at
            .map(|s| ((finished_at - s) * 1000.0) as i64)
            .unwrap_or(0);
        timeline.push(serde_json::json!({
            "event":     event,
            "at":        finished_at,
            "offset_ms": ((finished_at - t0) * 1000.0) as i64,
            "exec_ms":   exec_ms,
            "result":    job.result,
            "error":     job.error
        }));
    }

    Ok(Json(serde_json::json!({
        "job":      crate::models::JobView::from(job),
        "timeline": timeline
    })))
}

/// Get the full event log for a specific job.
#[utoipa::path(
    get, path = "/jobs/{id}/events",
    params(("id" = String, Path, description = "Job ID")),
    responses(
        (status = 200, description = "Event history for the job"),
    ),
    tag = "observability"
)]
pub async fn job_events(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let raw = tokio::task::spawn_blocking({
        let id = id.clone();
        move || space.events_for_job(&id)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    let events: Vec<serde_json::Value> = raw.into_iter().map(storage_event_to_value).collect();
    Ok(Json(serde_json::json!({
        "job_id":      id,
        "event_count": events.len(),
        "events":      events,
    })))
}

#[derive(Deserialize, IntoParams)]
pub struct ReplayQuery {
    /// Unix timestamp to reconstruct queue state at
    pub at: f64,
}

/// Reconstruct queue state at a past timestamp.
#[utoipa::path(
    get, path = "/replay",
    params(ReplayQuery),
    responses(
        (status = 200, description = "Queue snapshot at the requested timestamp"),
    ),
    tag = "observability"
)]
pub async fn replay_state(
    State(space): State<AppState>,
    Query(q): Query<ReplayQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let jobs = tokio::task::spawn_blocking(move || space.state_at(q.at))
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
    let views: Vec<JobView> = jobs.into_iter().map(JobView::from).collect();
    Ok(Json(serde_json::json!({
        "at":        q.at,
        "job_count": views.len(),
        "jobs":      views,
    })))
}

#[derive(Deserialize, IntoParams)]
pub struct ReplayEventsQuery {
    /// Start of time range (Unix timestamp)
    pub from:  f64,
    /// End of time range (Unix timestamp)
    pub to:    f64,
    /// Maximum number of events to return (default 1000)
    #[serde(default = "default_event_limit")]
    pub limit: usize,
}

fn default_event_limit() -> usize { 1000 }

/// List events in a time range.
#[utoipa::path(
    get, path = "/replay/events",
    params(ReplayEventsQuery),
    responses(
        (status = 200, description = "Events in the requested time range"),
    ),
    tag = "observability"
)]
pub async fn replay_events(
    State(space): State<AppState>,
    Query(q): Query<ReplayEventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let raw = tokio::task::spawn_blocking(move || {
        space.events_in_range(q.from, q.to, q.limit)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))??;

    let events: Vec<serde_json::Value> = raw.into_iter().map(storage_event_to_value).collect();
    Ok(Json(serde_json::json!({
        "from":        q.from,
        "to":          q.to,
        "event_count": events.len(),
        "events":      events,
    })))
}

fn storage_event_to_value(e: viscacha_storage::StorageEvent) -> serde_json::Value {
    let mut obj = serde_json::to_value(&e.op).unwrap_or_default();
    if let Some(map) = obj.as_object_mut() {
        map.insert("seq".into(),       serde_json::json!(e.seq));
        map.insert("timestamp".into(), serde_json::json!(e.timestamp));
    }
    obj
}

fn parse_status(s: &str) -> Result<viscacha_core::JobStatus, ApiError> {
    match s {
        "pending"   => Ok(viscacha_core::JobStatus::Pending),
        "claimed"   => Ok(viscacha_core::JobStatus::Claimed),
        "done"      => Ok(viscacha_core::JobStatus::Done),
        "failed"    => Ok(viscacha_core::JobStatus::Failed),
        "cancelled" => Ok(viscacha_core::JobStatus::Cancelled),
        other       => Err(ApiError::Conflict(format!("unknown status: {other}"))),
    }
}
