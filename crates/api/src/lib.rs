pub mod dashboard;
pub mod error;
pub mod models;
pub mod routes;

use std::sync::Arc;

use axum::routing::get;
use axum::routing::post;
use axum::Router;
use viscacha_storage::PersistentSpace;

use crate::dashboard::{dashboard_metrics, dashboard_page};
use crate::routes::{
    cancel_job, claim_job, complete_job, enqueue_job, fail_job, get_job, list_jobs,
};

pub fn router(space: Arc<PersistentSpace>) -> Router {
    Router::new()
        .route("/jobs",              post(enqueue_job).get(list_jobs))
        // /jobs/claim must be registered before /jobs/:id so the literal wins
        .route("/jobs/claim",          post(claim_job))
        .route("/jobs/{id}",          get(get_job))
        .route("/jobs/{id}/cancel",   post(cancel_job))
        .route("/jobs/{id}/complete", post(complete_job))
        .route("/jobs/{id}/fail",     post(fail_job))
        .route("/dashboard",          get(dashboard_page))
        .route("/dashboard/metrics",  get(dashboard_metrics))
        .with_state(space)
}

pub async fn run(db_path: Option<&str>, bind_addr: &str) {
    let space = Arc::new(match db_path {
        Some(p) => PersistentSpace::open(std::path::Path::new(p)).expect("failed to open db"),
        None    => PersistentSpace::open_in_memory().expect("failed to open in-memory db"),
    });

    let app = router(space);
    let listener = tokio::net::TcpListener::bind(bind_addr).await
        .unwrap_or_else(|e| panic!("failed to bind {bind_addr}: {e}"));

    println!("viscacha listening on {bind_addr}");
    axum::serve(listener, app).await.expect("server error");
}
