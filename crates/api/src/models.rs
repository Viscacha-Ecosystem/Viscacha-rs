use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use viscacha_core::Job;

// ── request bodies ────────────────────────────────────────────────────────────

#[derive(Deserialize, ToSchema)]
pub struct EnqueueBody {
    pub job_type:    String,
    #[serde(default = "default_max_retries")]
    pub max_retries: u8,
    #[serde(default)]
    pub args:        serde_json::Value,
}

fn default_max_retries() -> u8 { 3 }

#[derive(Deserialize, ToSchema)]
pub struct ClaimBody {
    pub job_type:  String,
    pub lease_ttl: f64,
    #[serde(default)]
    pub worker_id: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct CompleteBody {
    pub lease_id: String,
    pub result:   serde_json::Value,
}

#[derive(Deserialize, ToSchema)]
pub struct FailBody {
    pub lease_id: String,
    pub error:    String,
}

#[derive(Deserialize, ToSchema)]
pub struct HeartbeatBody {
    pub lease_id:    String,
    #[serde(default = "default_heartbeat_extend")]
    pub extend_secs: f64,
}

fn default_heartbeat_extend() -> f64 { 30.0 }

// ── response bodies ───────────────────────────────────────────────────────────

#[derive(Serialize, ToSchema)]
pub struct EnqueueResponse {
    pub job_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct ClaimResponse {
    pub job:      JobView,
    pub lease_id: String,
}

#[derive(Serialize, ToSchema)]
pub struct ListResponse {
    pub jobs: Vec<JobView>,
}

/// Serializes a Job in the shape the Python SDK's JobResult._from_dict() expects.
#[derive(Serialize, ToSchema)]
pub struct JobView {
    pub id:          String,
    pub status:      String,
    pub job_type:    String,
    pub args:        serde_json::Value,
    pub result:      Option<serde_json::Value>,
    pub error:       Option<String>,
    pub retries:     u8,
    pub max_retries: u8,
    pub enqueued_at: Option<f64>,
    pub started_at:  Option<f64>,
    pub finished_at: Option<f64>,
    pub worker_id:   Option<String>,
}

impl From<Job> for JobView {
    fn from(j: Job) -> Self {
        JobView {
            id:          j.id.0,
            status:      j.status.to_string(),
            job_type:    j.job_type,
            args:        j.args,
            result:      j.result,
            error:       j.error,
            retries:     j.retries,
            max_retries: j.max_retries,
            enqueued_at: Some(j.enqueued_at),
            started_at:  j.started_at,
            finished_at: j.finished_at,
            worker_id:   j.worker_id,
        }
    }
}
