use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use viscacha_core::JobId;
use viscacha_storage::PersistentSpace;

use crate::error::ApiError;
use crate::models::{
    ClaimBody, ClaimResponse, CompleteBody, EnqueueBody, EnqueueResponse, FailBody, JobView,
    ListResponse,
};

pub type AppState = Arc<PersistentSpace>;

// POST /jobs
pub async fn enqueue_job(
    State(space): State<AppState>,
    Json(body): Json<EnqueueBody>,
) -> Result<(StatusCode, Json<EnqueueResponse>), ApiError> {
    let job_id = space.enqueue(body.job_type, body.args, body.max_retries)?;
    Ok((StatusCode::CREATED, Json(EnqueueResponse { job_id: job_id.0 })))
}

// GET /jobs/:id
pub async fn get_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobView>, ApiError> {
    let job = space.get(&JobId(id))
        .ok_or_else(|| ApiError::NotFound("job not found".into()))?;
    Ok(Json(JobView::from(job)))
}

// POST /jobs/:id/cancel
pub async fn cancel_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    space.cancel(&JobId(id)).map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({ "status": "cancelled" })))
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub status: Option<String>,
}

// GET /jobs
pub async fn list_jobs(
    State(space): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<ListResponse>, ApiError> {
    let status = q.status.as_deref().map(parse_status).transpose()?;
    let jobs = space.list(status).into_iter().map(JobView::from).collect();
    Ok(Json(ListResponse { jobs }))
}

// POST /jobs/claim
pub async fn claim_job(
    State(space): State<AppState>,
    Json(body): Json<ClaimBody>,
) -> Result<impl IntoResponse, ApiError> {
    match space.claim(&body.job_type, body.lease_ttl)? {
        None => Ok(StatusCode::NO_CONTENT.into_response()),
        Some((job, lease_id)) => Ok(Json(ClaimResponse {
            job:      JobView::from(job),
            lease_id: lease_id.0,
        })
        .into_response()),
    }
}

// POST /jobs/:id/complete
pub async fn complete_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<CompleteBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::LeaseId;
    space
        .complete(&JobId(id), &LeaseId(body.lease_id), body.result)
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({ "status": "done" })))
}

// POST /jobs/:id/fail
pub async fn fail_job(
    State(space): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<FailBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    use viscacha_core::LeaseId;
    space
        .fail(&JobId(id), &LeaseId(body.lease_id), body.error)
        .map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({ "status": "ok" })))
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
