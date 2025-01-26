use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;
use dashmap::DashMap;

type TimeoutDB = Arc<DashMap<String, SystemTime>>;
type SharedDB = Arc<DashMap<String, Vec<u8>>>;

const BATCH_SIZE: usize = 100;
const CLEANUP_INTERVAL: Duration = Duration::from_millis(250);

pub async fn cache_manager(shutdown_token: CancellationToken, timeout_db: TimeoutDB, shared_db: SharedDB) {
    let mut last_metrics_log = Instant::now();
    let mut total_items_cleaned = 0;

    loop {
        if shutdown_token.is_cancelled() {
            break;
        }

        let cleanup_start = Instant::now();
        let items_cleaned = cleanup_expired_entries(&timeout_db, &shared_db);
        total_items_cleaned += items_cleaned;

        // Log metrics every minute
        if last_metrics_log.elapsed() >= Duration::from_secs(60) {
            tracing::debug!(
                "Cache metrics: total_entries={}, cleaned_last_minute={}",
                shared_db.len(),
                total_items_cleaned
            );
            total_items_cleaned = 0;
            last_metrics_log = Instant::now();
        }

        // Adaptive sleep to maintain consistent cleanup interval
        let elapsed = cleanup_start.elapsed();
        if elapsed < CLEANUP_INTERVAL {
            sleep(CLEANUP_INTERVAL - elapsed).await;
        }
    }
    tracing::debug!("Cache manager shutdown complete");
}

fn cleanup_expired_entries(timeout_db: &TimeoutDB, shared_db: &SharedDB) -> usize {
    let now = SystemTime::now();
    let mut remove_keys = Vec::with_capacity(BATCH_SIZE);
    let mut cleaned_count = 0;

    // Collect expired keys
    for entry in timeout_db.iter() {
        if now > *entry.value() {
            remove_keys.push(entry.key().clone());
            cleaned_count += 1;
        }

        if remove_keys.len() >= BATCH_SIZE {
            break;
        }
    }

    // Remove expired entries
    for key in remove_keys {
        shared_db.remove(&key);
        timeout_db.remove(&key);
    }

    cleaned_count
}
