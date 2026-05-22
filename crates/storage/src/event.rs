use serde::{Deserialize, Serialize};

/// Every mutation to the space is recorded as one of these ops.
/// This is the durable source of truth — the in-memory TupleSpace is a projection of it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum EventOp {
    Enqueue {
        job_id:      String,
        job_type:    String,
        args:        serde_json::Value,
        max_retries: u8,
    },
    Claim {
        job_id:      String,
        lease_id:    String,
        lease_until: f64,
        worker_id:   Option<String>,
    },
    Complete {
        job_id:   String,
        lease_id: String,
        result:   serde_json::Value,
    },
    Fail {
        job_id:   String,
        lease_id: String,
        error:    String,
    },
    Cancel {
        job_id: String,
    },
    Expire {
        job_id: String,
    },
    Purge {
        job_id: String,
    },
    Retry {
        job_id: String,
    },
}

/// A persisted event with its position in the log.
#[derive(Debug, Clone)]
pub struct StorageEvent {
    pub seq:       i64,
    pub timestamp: f64,
    pub op:        EventOp,
}
