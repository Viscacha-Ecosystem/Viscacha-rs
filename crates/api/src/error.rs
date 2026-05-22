use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use viscacha_core::SpaceError;
use viscacha_storage::StorageError;

pub enum ApiError {
    NotFound(String),
    Conflict(String),
    Internal(String),
}

impl From<SpaceError> for ApiError {
    fn from(e: SpaceError) -> Self {
        match e {
            SpaceError::NotFound(_) => ApiError::NotFound(e.to_string()),
            SpaceError::NotPending(_)
            | SpaceError::NotClaimed(_)
            | SpaceError::LeaseInvalid
            | SpaceError::InvalidTransition { .. } => ApiError::Conflict(e.to_string()),
            SpaceError::Io(_) => ApiError::Internal(e.to_string()),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        ApiError::Internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound(m)  => (StatusCode::NOT_FOUND, m),
            ApiError::Conflict(m)  => (StatusCode::CONFLICT, m),
            ApiError::Internal(m)  => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
