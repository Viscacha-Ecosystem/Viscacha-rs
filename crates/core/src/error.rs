use thiserror::Error;
use crate::types::JobId;

#[derive(Debug, Error)]
pub enum SpaceError {
    #[error("job {0:?} not found")]
    NotFound(JobId),

    #[error("job {0:?} is not in Pending state")]
    NotPending(JobId),

    #[error("job {0:?} is not in Claimed state")]
    NotClaimed(JobId),

    #[error("lease has expired or does not match")]
    LeaseInvalid,

    #[error("invalid state transition: {from} -> {to}")]
    InvalidTransition { from: String, to: String },

    #[error("storage error: {0}")]
    Io(String),
}
