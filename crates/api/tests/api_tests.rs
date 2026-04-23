use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use viscacha_storage::PersistentSpace;

fn test_router() -> axum::Router {
    let space = Arc::new(PersistentSpace::open_in_memory().unwrap());
    viscacha_api::router(space)
}

async fn post(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

// ── enqueue ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn enqueue_returns_job_id() {
    let app = test_router();
    let (status, body) = post(&app, "/jobs", json!({
        "job_type": "send_email",
        "args": { "to": "a@b.com" }
    })).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(body["job_id"].is_string());
}

#[tokio::test]
async fn enqueue_then_get() {
    let app = test_router();
    let (_, enq) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;
    let job_id = enq["job_id"].as_str().unwrap();

    let (status, job) = get(&app, &format!("/jobs/{job_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(job["status"], "pending");
    assert_eq!(job["job_type"], "ocr");
}

#[tokio::test]
async fn get_unknown_job_is_404() {
    let app = test_router();
    let (status, _) = get(&app, "/jobs/does-not-exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ── list ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn list_returns_all_jobs() {
    let app = test_router();
    post(&app, "/jobs", json!({ "job_type": "a", "args": {} })).await;
    post(&app, "/jobs", json!({ "job_type": "b", "args": {} })).await;
    let (status, body) = get(&app, "/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_filters_by_status() {
    let app = test_router();
    post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;
    let (_, enq2) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;

    // Cancel one
    let id2 = enq2["job_id"].as_str().unwrap();
    post(&app, &format!("/jobs/{id2}/cancel"), json!({})).await;

    let (_, pending) = get(&app, "/jobs?status=pending").await;
    assert_eq!(pending["jobs"].as_array().unwrap().len(), 1);

    let (_, cancelled) = get(&app, "/jobs?status=cancelled").await;
    assert_eq!(cancelled["jobs"].as_array().unwrap().len(), 1);
}

// ── cancel ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn cancel_pending_job() {
    let app = test_router();
    let (_, enq) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;
    let id = enq["job_id"].as_str().unwrap();

    let (status, body) = post(&app, &format!("/jobs/{id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "cancelled");

    let (_, job) = get(&app, &format!("/jobs/{id}")).await;
    assert_eq!(job["status"], "cancelled");
}

#[tokio::test]
async fn cancel_done_job_is_conflict() {
    let app = test_router();
    let (_, enq) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;
    let id = enq["job_id"].as_str().unwrap();

    // Claim then complete
    let (_, claim) = post(&app, "/jobs/claim", json!({ "job_type": "ocr", "lease_ttl": 30.0 })).await;
    let lease_id = claim["lease_id"].as_str().unwrap();
    post(&app, &format!("/jobs/{id}/complete"), json!({ "lease_id": lease_id, "result": null })).await;

    let (status, _) = post(&app, &format!("/jobs/{id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

// ── claim / complete / fail ───────────────────────────────────────────────────

#[tokio::test]
async fn claim_returns_204_when_empty() {
    let app = test_router();
    let req = Request::builder()
        .method("POST")
        .uri("/jobs/claim")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "job_type": "ocr", "lease_ttl": 30.0 }).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn claim_complete_full_cycle() {
    let app = test_router();
    let (_, enq) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": { "file": "x.pdf" } })).await;
    let id = enq["job_id"].as_str().unwrap();

    let (status, claim) = post(&app, "/jobs/claim", json!({ "job_type": "ocr", "lease_ttl": 30.0 })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(claim["job"]["id"], id);
    let lease_id = claim["lease_id"].as_str().unwrap();

    let (status, _) = post(&app, &format!("/jobs/{id}/complete"), json!({
        "lease_id": lease_id,
        "result": { "pages": 3 }
    })).await;
    assert_eq!(status, StatusCode::OK);

    let (_, job) = get(&app, &format!("/jobs/{id}")).await;
    assert_eq!(job["status"], "done");
    assert_eq!(job["result"]["pages"], 3);
}

#[tokio::test]
async fn fail_requeues_with_retries_remaining() {
    let app = test_router();
    post(&app, "/jobs", json!({ "job_type": "ocr", "max_retries": 3, "args": {} })).await;

    let (_, claim) = post(&app, "/jobs/claim", json!({ "job_type": "ocr", "lease_ttl": 30.0 })).await;
    let id = claim["job"]["id"].as_str().unwrap();
    let lease_id = claim["lease_id"].as_str().unwrap();

    let (status, _) = post(&app, &format!("/jobs/{id}/fail"), json!({
        "lease_id": lease_id,
        "error": "timed out"
    })).await;
    assert_eq!(status, StatusCode::OK);

    let (_, job) = get(&app, &format!("/jobs/{id}")).await;
    assert_eq!(job["status"], "pending");
    assert_eq!(job["retries"], 1);
}

#[tokio::test]
async fn complete_with_wrong_lease_is_conflict() {
    let app = test_router();
    let (_, enq) = post(&app, "/jobs", json!({ "job_type": "ocr", "args": {} })).await;
    let id = enq["job_id"].as_str().unwrap();
    post(&app, "/jobs/claim", json!({ "job_type": "ocr", "lease_ttl": 30.0 })).await;

    let (status, _) = post(&app, &format!("/jobs/{id}/complete"), json!({
        "lease_id": "wrong-lease-id",
        "result": null
    })).await;
    assert_eq!(status, StatusCode::CONFLICT);
}
