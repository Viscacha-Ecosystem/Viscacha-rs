use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseId(pub String);

impl LeaseId {
    pub fn new() -> Self {
        LeaseId(Uuid::new_v4().to_string())
    }
}

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Claimed,
    Done,
    Failed,
    Cancelled,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JobStatus::Pending   => "pending",
            JobStatus::Claimed   => "claimed",
            JobStatus::Done      => "done",
            JobStatus::Failed    => "failed",
            JobStatus::Cancelled => "cancelled",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id:          JobId,
    pub job_type:    String,
    pub args:        serde_json::Value,
    pub status:      JobStatus,
    pub retries:     u8,
    pub max_retries: u8,
    /// Unix timestamp seconds
    pub enqueued_at: f64,
    pub started_at:  Option<f64>,
    pub finished_at: Option<f64>,
    pub result:      Option<serde_json::Value>,
    pub error:       Option<String>,
    /// Unix timestamp; Some while Claimed, None otherwise
    pub lease_until: Option<f64>,
    pub lease_id:    Option<LeaseId>,
    pub worker_id:   Option<String>,
}

impl Job {
    pub fn new(id: JobId, job_type: String, args: serde_json::Value, max_retries: u8, now: f64) -> Self {
        Job {
            id,
            job_type,
            args,
            status:      JobStatus::Pending,
            retries:     0,
            max_retries,
            enqueued_at: now,
            started_at:  None,
            finished_at: None,
            result:      None,
            error:       None,
            lease_until: None,
            lease_id:    None,
            worker_id:   None,
        }
    }
}
