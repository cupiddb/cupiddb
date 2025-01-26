use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use dashmap::DashMap;
use tokio::net::{TcpStream, TcpListener};
use tokio::time::{sleep, Duration};
use tokio::signal::unix::{signal, SignalKind};
use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::config::AppConfig;
use crate::handler::handler::handle_frame;
use crate::handler::cache_manager::cache_manager;
use crate::handler::connection::Connection;

type TimeoutDB = Arc<DashMap<String, SystemTime>>;
type SharedDB = Arc<DashMap<String, Vec<u8>>>;

pub struct Server {
    listener: TcpListener,
    config: AppConfig,
}

impl Server {
    pub async fn new(config: AppConfig) -> Result<Server, std::io::Error> {
        let listener = TcpListener::bind(config.bind_address.as_str()).await?;
        Ok(Server {
            listener,
            config,
        })
    }

    pub async fn run(self) {
        let shutdown_token = CancellationToken::new();
        let timeout_db = Arc::new(DashMap::with_capacity_and_shard_amount(
            self.config.cache_initial_capacity,
            self.config.cache_shards
        ));
        let shared_db = Arc::new(DashMap::with_capacity_and_shard_amount(
            self.config.cache_initial_capacity,
            self.config.cache_shards
        ));

        // Spawn cache manager
        let cache_manager_handle = {
            let cloned_token = shutdown_token.clone();
            let cloned_timeout_db = Arc::clone(&timeout_db);
            let cloned_db = Arc::clone(&shared_db);
            tokio::spawn(async move {
                cache_manager(cloned_token, cloned_timeout_db, cloned_db).await;
            })
        };

        // Improved signal handling
        let shutdown_handle = {
            let cloned_token = shutdown_token.clone();
            tokio::spawn(async move {
                let mut signal_terminate = signal(SignalKind::terminate()).unwrap();
                let mut signal_interrupt = signal(SignalKind::interrupt()).unwrap();
                tokio::select! {
                    _ = signal_terminate.recv() => tracing::info!("Received SIGTERM"),
                    _ = signal_interrupt.recv() => tracing::info!("Received SIGINT"),
                };
                tracing::info!("Initiating shutdown sequence");
                cloned_token.cancel();
            })
        };

        let connection_counter = Arc::new(Mutex::new(0_usize));

        // Main accept loop
        loop {
            let accept_result = select! {
                res = self.listener.accept() => res,
                _ = shutdown_token.cancelled() => {
                    tracing::info!("Shutdown signal received, stopping accept loop");
                    break;
                }
            };

            match accept_result {
                Ok((socket, addr)) => {
                    if let Err(e) = socket.set_nodelay(true) {
                        tracing::warn!("Failed to set TCP_NODELAY: {}", e);
                    }
                    tracing::debug!("Accepted client with address {}", addr);

                    let mut counter = connection_counter.lock().unwrap();
                    *counter += 1;
                    drop(counter);

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
                Err(e) => {
                    tracing::error!("Failed to accept connection: {}", e);
                    continue;
                }
            }
        }

        // Graceful shutdown
        self.graceful_shutdown(connection_counter, self.config.graceful_timeout).await;

        // Wait for background tasks
        let _ = tokio::join!(cache_manager_handle, shutdown_handle);
        tracing::info!("Server shutdown complete");
    }

    async fn graceful_shutdown(&self, connection_counter: Arc<Mutex<usize>>, timeout_seconds: usize) {
        tracing::info!("Gracefully shutting down with a {} second timeout", timeout_seconds);

        for remaining in (0..timeout_seconds).rev() {
            let count = {
                let counter = connection_counter.lock().unwrap();
                *counter
            };

            if count == 0 {
                break;
            }

            tracing::info!("Waiting for {} connections to close, {} seconds remaining",
                count, remaining);
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn handle_stream(socket: TcpStream, token: CancellationToken, timeout_db: TimeoutDB, shared_db: SharedDB) {
    tracing::debug!("Client accepted");
    let mut connection = Connection::new(socket);

    loop {
        let (message_type, payload) = select! {
            res = connection.read_frame() => res,
            _ = token.cancelled() => {
                ("CC".to_string(), vec![0; 0])
            }
        };
        let cloned_timeout_db = Arc::clone(&timeout_db);
        let cloned_db = Arc::clone(&shared_db);

        let (response_type, response_payload) = handle_frame(&message_type, &payload, cloned_timeout_db, cloned_db);

        if response_type == "CC" || message_type == "WP" {
            connection.write_frame(response_type, response_payload).await;
            break;
        }

        connection.write_frame(response_type, response_payload).await;
    }
    tracing::debug!("End connection");
}
