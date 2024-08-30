use std::sync::Arc;
use std::time::SystemTime;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use dashmap::DashMap;

type TimeoutDB = Arc<DashMap<String, SystemTime>>;
type SharedDB = Arc<DashMap<String, Vec<u8>>>;


pub async fn cache_manager(shutdown_token: CancellationToken, timeout_db: TimeoutDB, shared_db: SharedDB) {
    loop {
        if shutdown_token.is_cancelled() {
            break;
        }
        let now = SystemTime::now();
        let mut remove_keys: Vec<String> = Vec::new();
        for entry in timeout_db.iter() {
            if now > *entry.value() {
                remove_keys.push(entry.key().clone());
            }
        }
        for key in remove_keys {
            shared_db.remove(&key);
            timeout_db.remove(&key);
        }

        sleep(Duration::from_millis(250)).await;
    }
    tracing::debug!("Stopped cache manager");
}
