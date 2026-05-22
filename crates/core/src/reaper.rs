use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;

use crate::space::TupleSpace;

/// Spawns a background tokio task that expires stale leases every interval.
pub fn spawn_lease_reaper(space: Arc<TupleSpace>, interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        loop {
            ticker.tick().await;
            let now = unix_now();
            let expired = space.expire_leases(now);
            if expired > 0 {
                tracing_or_print(expired);
            }
        }
    })
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn tracing_or_print(n: usize) {
    // Will wire to `tracing` crate when api crate is built.
    // For now imma just a no-op placeholder so core has zero non-workspace deps.
    let _ = n;
}
