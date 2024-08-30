use std::env;
use std::thread::available_parallelism;
use tracing::{subscriber, Level};

pub struct AppConfig {
    pub worker_threads: usize,
    pub bind_address: String,
    pub cache_initial_capacity: usize,
    pub cache_shards: usize,
    pub graceful_timeout: usize,
}

impl AppConfig {
    pub fn new() -> AppConfig {
        // Logging
        let log_level: Level = match env::var("CUPID_LOG_LEVEL") {
            Ok(val) => {
                match val.as_str() {
                    "ERROR" => Level::ERROR,
                    "WARN" => Level::WARN,
                    "INFO" => Level::INFO,
                    "DEBUG" => Level::DEBUG,
                    "TRACE" => Level::TRACE,
                    _ => Level::INFO,
                }
            }
            Err(_) => Level::INFO,
        };
        let debug_mode: bool;
        if log_level == Level::DEBUG || log_level == Level::TRACE {
            debug_mode = true;
        } else {
            debug_mode = false;
        }

        let subscriber = tracing_subscriber::fmt()
            .compact()
            .with_file(debug_mode)
            .with_line_number(debug_mode)
            .with_thread_ids(debug_mode)
            .with_target(false)
            .with_max_level(log_level)
            .finish();
        let _ = subscriber::set_global_default(subscriber);
        tracing::info!("Starting CupidDB");
        tracing::info!("Log level set to {log_level}");

        // Tokio worker threads
        let worker_threads: usize = match env::var("CUPID_WORKER_THREADS") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => available_parallelism().unwrap().get(),
        };
        tracing::info!("Starting CupidDB with {worker_threads} threads");

        // Cache
        let cache_initial_capacity: usize = match env::var("CUPID_INITIAL_CAPACITY") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 64,
        };
        let cache_shards: usize = match env::var("CUPID_CACHE_SHARDS") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 64,
        };
        tracing::info!("Running with {cache_shards} shards");

        // Graceful timeout
        let graceful_timeout: usize = match env::var("CUPID_GRACEFUL_TIMEOUT") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 30,
        };

        // Network
        let address: String = match env::var("CUPID_BIND_ADDRESS") {
            Ok(val) => val,
            Err(_) => "0.0.0.0".to_string(),
        };
        let port: u16 = match env::var("CUPID_PORT") {
            Ok(val) => val.parse().unwrap(),
            Err(_) => 5995,
        };
        let bind_address = address + ":" + port.to_string().as_str();
        tracing::info!("Listening on {bind_address}");

        return AppConfig {
            worker_threads: worker_threads,
            bind_address: bind_address,
            cache_initial_capacity: cache_initial_capacity,
            cache_shards: cache_shards,
            graceful_timeout: graceful_timeout,
        }
    }
}
