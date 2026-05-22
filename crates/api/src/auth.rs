use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Bearer-token auth middleware.
///
/// If VISCACHA_API_KEY is set, every request must carry
/// `Authorization: Bearer <key>`. If the env var is absent or empty,
/// all requests are allowed (development mode).
/// The UI path ("/") is always allowed so the browser can load the app.
pub async fn require_api_key(req: Request, next: Next) -> Response {
    // Public paths: UI (browser page load) and OpenAPI spec (tooling / docs)
    if matches!(req.uri().path(), "/" | "/openapi.json") {
        return next.run(req).await;
    }

    let expected = match std::env::var("VISCACHA_API_KEY") {
        Ok(k) if !k.is_empty() => k,
        _ => return next.run(req).await,
    };

    let authorized = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map_or(false, |token| token == expected);

    if authorized {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": "unauthorized" }))).into_response()
    }
}
