use utoipa::OpenApi;

use crate::models::{
    ClaimBody, ClaimResponse, CompleteBody, EnqueueBody, EnqueueResponse, FailBody, HeartbeatBody,
    JobView, ListResponse,
};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Viscacha",
        version = "0.1.0",
        description = "High-performance job queue server. \
            Every operation is appended to a SQLite WAL event log before touching \
            in-memory state; crash-safe by design.",
    ),
    paths(
        crate::routes::enqueue_job,
        crate::routes::list_jobs,
        crate::routes::get_job,
        crate::routes::cancel_job,
        crate::routes::retry_job,
        crate::routes::claim_job,
        crate::routes::complete_job,
        crate::routes::fail_job,
        crate::routes::heartbeat_job,
        crate::routes::trace_job,
        crate::routes::job_events,
        crate::routes::replay_state,
        crate::routes::replay_events,
    ),
    components(schemas(
        EnqueueBody,
        ClaimBody,
        CompleteBody,
        FailBody,
        HeartbeatBody,
        EnqueueResponse,
        ClaimResponse,
        ListResponse,
        JobView,
    )),
    tags(
        (name = "jobs",          description = "Enqueue, inspect, cancel, and retry jobs"),
        (name = "workers",       description = "Worker protocol: claim, complete, fail, heartbeat"),
        (name = "observability", description = "Trace timelines, event log, and time-travel replay"),
    ),
)]
pub struct ApiDoc;

pub async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    axum::Json(ApiDoc::openapi())
}
