use tokio::runtime::Builder;

mod config;
mod server;
mod handler;
use crate::config::AppConfig;
use crate::server::Server;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    let config = AppConfig::new();

    let runtime = Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .worker_threads(config.worker_threads)
        .build()
        .unwrap();

    let handle = runtime.spawn(start_server(config));

    runtime.block_on(handle).unwrap();
}

async fn start_server(config: AppConfig) {
    let server = match Server::new(config).await {
        Ok(server) => server,
        Err(e) => {
            tracing::error!("Failed to create server: {}", e);
            std::process::exit(1);
        }
    };

    server.run().await;
}
