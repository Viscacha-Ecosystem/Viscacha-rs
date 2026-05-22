pub mod auth;
pub mod dashboard;
pub mod error;
pub mod metrics;
pub mod models;
pub mod openapi;
pub mod routes;
pub mod ui;

use std::sync::Arc;
use std::time::Duration;

use axum::middleware;
use axum::routing::get;
use axum::routing::post;
use axum::Router;
use viscacha_storage::PersistentSpace;

use crate::dashboard::{dashboard_metrics, dashboard_page};
use crate::openapi::openapi_json;
use crate::ui::ui_page;
use crate::metrics::prometheus_metrics;
use crate::routes::{
    cancel_job, claim_job, complete_job, enqueue_job, fail_job, get_job, heartbeat_job, job_events,
    list_jobs, replay_events, replay_state, retry_job, trace_job,
};

pub fn router(space: Arc<PersistentSpace>) -> Router {
    Router::new()
        .route("/",                   get(ui_page))
        .route("/jobs",              post(enqueue_job).get(list_jobs))
        // /jobs/claim must be registered before /jobs/:id so the literal wins
        .route("/jobs/claim",          post(claim_job))
        .route("/jobs/{id}",          get(get_job))
        .route("/jobs/{id}/cancel",   post(cancel_job))
        .route("/jobs/{id}/complete", post(complete_job))
        .route("/jobs/{id}/fail",     post(fail_job))
        .route("/jobs/{id}/trace",     get(trace_job))
        .route("/jobs/{id}/retry",     post(retry_job))
        .route("/jobs/{id}/heartbeat", post(heartbeat_job))
        .route("/jobs/{id}/events",    get(job_events))
        .route("/replay",              get(replay_state))
        .route("/replay/events",       get(replay_events))
        .route("/dashboard",           get(dashboard_page))
        .route("/dashboard/metrics",  get(dashboard_metrics))
        .route("/metrics",            get(prometheus_metrics))
        .route("/openapi.json",       get(openapi_json))
        .with_state(space)
        .layer(middleware::from_fn(auth::require_api_key))
}

pub async fn run(db_path: Option<&str>, bind_addr: &str) {
    let space = Arc::new(match db_path {
        Some(p) => PersistentSpace::open(std::path::Path::new(p)).expect("failed to open db"),
        None    => PersistentSpace::open_in_memory().expect("failed to open in-memory db"),
    });

    if std::env::var("VISCACHA_API_KEY").map_or(true, |k| k.is_empty()) {
        eprintln!("warning: VISCACHA_API_KEY not set — API is unauthenticated");
    }

    // Lease reaper: expire stale leases back to Pending every 5 s
    {
        let s = Arc::clone(&space);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticker.tick().await;
                let s2 = Arc::clone(&s);
                tokio::task::spawn_blocking(move || { s2.expire_leases(); }).await.ok();
            }
        });
    }

    // Hourly cleanup: purge terminal jobs older than 24 h, then compact the log
    {
        let s = Arc::clone(&space);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                let s2 = Arc::clone(&s);
                let res = tokio::task::spawn_blocking(move || -> viscacha_storage::error::Result<usize> {
                    let n = s2.purge_terminal(86_400.0)?;
                    s2.snapshot()?;
                    Ok(n)
                }).await;
                match res {
                    Ok(Ok(n)) if n > 0 => println!("purged {n} terminal jobs"),
                    Ok(Err(e))         => eprintln!("cleanup failed: {e}"),
                    Err(e)             => eprintln!("cleanup task panicked: {e}"),
                    _                  => {}
                }
            }
        });
    }

    let space_shutdown = Arc::clone(&space);
    let app = router(space);
    let listener = tokio::net::TcpListener::bind(bind_addr).await
        .unwrap_or_else(|e| panic!("failed to bind {bind_addr}: {e}"));

    println!("viscacha listening on {bind_addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    // Drain in-flight leases and persist before exiting
    let n = tokio::task::spawn_blocking(move || {
        let n = space_shutdown.drain_claimed().unwrap_or(0);
        let _ = space_shutdown.snapshot();
        n
    })
    .await
    .unwrap_or(0);
    if n > 0 {
        println!("shutdown: returned {n} in-flight job(s) to pending");
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate())
        .expect("failed to listen for SIGTERM");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl+c");
}
