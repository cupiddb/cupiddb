use std::sync::{Arc, Mutex};
use dashmap::DashMap;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use tokio::signal::unix::{signal, SignalKind};
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::handler::handler::handle_stream;
use crate::handler::cache_manager::cache_manager;

pub struct Server {
    listener: TcpListener,
    config: AppConfig,
}

impl Server {
    pub async fn new(config: AppConfig) -> Server {
        Server {
            listener: TcpListener::bind(config.bind_address.as_str()).await.unwrap(),
            config: config,
        }
    }

    pub async fn run(self) {
        let shutdown_token = CancellationToken::new();

        let timeout_db = Arc::new(DashMap::with_capacity_and_shard_amount(
            self.config.cache_initial_capacity, self.config.cache_shards
        ));
        let shared_db = Arc::new(DashMap::with_capacity_and_shard_amount(
            self.config.cache_initial_capacity, self.config.cache_shards
        ));
        let cloned_timeout_db = Arc::clone(&timeout_db);
        let cloned_db = Arc::clone(&shared_db);
        let cloned_token = shutdown_token.clone();

        tokio::spawn(async move {
            cache_manager(cloned_token, cloned_timeout_db, cloned_db).await;
        });

        let cloned_cancel_token = shutdown_token.clone();
        tokio::spawn(async move {
            let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
            let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();
            tokio::select! {
                _ = signal_terminate.recv() => tracing::info!("Received SIGTERM"),
                _ = signal_interrupt.recv() => tracing::info!("Received SIGINT"),
            };
            cloned_cancel_token.cancel();
        });

        let connection_counter = Arc::new(Mutex::new(0 as usize));
        loop {
            let (socket, addr) = select! {
                res = self.listener.accept() => {
                    let mut counter = connection_counter.lock().unwrap();
                    *counter += 1;
                    res.unwrap()
                }
                _ = shutdown_token.cancelled() => {
                    break; // Stop accepting new connections
                }
            };
            let _ = socket.set_nodelay(true);
            tracing::debug!("Accepted client with address {}", addr);

            let counter_clone = Arc::clone(&connection_counter);
            let cloned_token = shutdown_token.clone();
            let cloned_timeout_db = Arc::clone(&timeout_db);
            let cloned_db = Arc::clone(&shared_db);

            tokio::spawn(async move {
                handle_stream(socket, cloned_token, cloned_timeout_db, cloned_db).await;
                let mut counter = counter_clone.lock().unwrap();
                *counter -= 1;
            });
        }

        tracing::info!("Gracefully shutting down with a {} second timeout.", self.config.graceful_timeout);
        for _ in 0..self.config.graceful_timeout {
            {
                let counter = connection_counter.lock().unwrap();
                if *counter == 0 {
                    break;
                }
            }
            sleep(Duration::from_millis(1000)).await;
        }
        tracing::info!("Exiting");
    }
}
