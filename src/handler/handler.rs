use std::sync::Arc;
use std::time::{SystemTime, Duration};

use tokio::net::TcpStream;
use tokio::select;
use tokio_util::sync::CancellationToken;
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{StreamWriter, IpcWriteOptions};
use arrow::ipc::CompressionType;
use arrow::ipc::gen::Schema::MetadataVersion;
use dashmap::DashMap;
use serde::Deserialize;

use crate::handler::connection::Connection;
use crate::handler::filterer::process_filter;

type TimeoutDB = Arc<DashMap<String, SystemTime>>;
type SharedDB = Arc<DashMap<String, Vec<u8>>>;

#[derive(Deserialize)]
struct Query {
    key: String,
    columns: Vec<String>,
    filterlogic: String,
    filter: Vec<ColumnFilter>,
    cachetime: u64,
    compression_type: String,
}

#[derive(Deserialize)]
pub struct ColumnFilter {
    pub col: String,
    pub filter_type: String,
    pub data_type: String,
    pub value: f64,
}

pub async fn handle_stream(socket: TcpStream, token: CancellationToken, timeout_db: TimeoutDB, shared_db: SharedDB) {
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

        let (response_type, response_payload) = match message_type.as_str() {
            "SD" => handle_set_data(cloned_timeout_db, payload, cloned_db).await,
            "II" => handle_increment_integer(payload, cloned_db).await,
            "IF" => handle_increment_float(payload, cloned_db).await,
            "GA" => handle_get_arrow_data(cloned_timeout_db, payload, cloned_db).await,
            "GD" => handle_get_data(payload, cloned_db).await,
            "DL" => handle_delete(cloned_timeout_db, payload, cloned_db).await,
            "TH" => handle_touch(cloned_timeout_db, payload, cloned_db).await,
            "TL" => handle_ttl(cloned_timeout_db, payload, cloned_db).await,
            "LS" => handle_list_keys(cloned_db).await,
            "DM" => handle_delete_many(cloned_timeout_db, payload, cloned_db).await,
            "CC" => handle_connection_close().await,
            _ => handle_unknown_type().await,
        };
        if response_type == "CC" {
            break;
        }

        connection.write_frame(response_type, response_payload).await;
    }
    tracing::debug!("End connection");
}

async fn handle_set_data(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let cache_time_bytes: [u8; 8] = payload[0..8].try_into().expect("Incorrect length");
    let cache_time_ms = u64::from_be_bytes(cache_time_bytes);
    let key_index_until = (u16::from_be_bytes([payload[8], payload[9]]) + 10) as usize;
    let key = match std::str::from_utf8(&payload[10..key_index_until]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    shared_db.insert(key.clone(), payload[key_index_until..].to_vec());
    if cache_time_ms > 0 {
        let now = SystemTime::now();
        let duration = Duration::from_millis(cache_time_ms);
        timeout_db.insert(key, now + duration);
    } else {
        let _ = timeout_db.remove(&key);
    }
    return ("OK".to_string(), vec![0; 0]);
}

async fn handle_increment_integer(payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let key = match std::str::from_utf8(&payload[8..]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    match shared_db.entry(key.clone()) {
        dashmap::Entry::Occupied(mut entry) => {
            let int_bytes = entry.get_mut();
            if int_bytes[0] as char != 'I' || int_bytes.len() != 9 {
                let error_code: u16 = 5;
                return ("ER".to_string(), error_code.to_be_bytes().to_vec());
            }
            let mut int_data = i64::from_be_bytes(int_bytes[1..].try_into().unwrap());
            let increment_amount = i64::from_be_bytes(payload[0..8].try_into().unwrap());
            int_data += increment_amount;

            int_bytes[1..].clone_from_slice(&int_data.to_be_bytes());
            return ("IN".to_string(), int_data.to_be_bytes().to_vec());
        }
        dashmap::Entry::Vacant(entry) => {
            let mut int_bytes_vec = payload[0..8].to_vec();
            int_bytes_vec.insert(0, 'I' as u8);

            entry.insert(int_bytes_vec.clone());
            return ("IN".to_string(), int_bytes_vec[1..].to_vec());
        }
    }
}

async fn handle_increment_float(payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let key = match std::str::from_utf8(&payload[8..]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    match shared_db.entry(key.clone()) {
        dashmap::Entry::Occupied(mut entry) => {
            let float_bytes = entry.get_mut();
            if float_bytes[0] as char != 'F' || float_bytes.len() != 9 {
                let error_code: u16 = 5;
                return ("ER".to_string(), error_code.to_be_bytes().to_vec());
            }
            let mut float_data = f64::from_be_bytes(float_bytes[1..].try_into().unwrap());
            let increment_amount = f64::from_be_bytes(payload[0..8].try_into().unwrap());
            float_data += increment_amount;

            float_bytes[1..].clone_from_slice(&float_data.to_be_bytes());
            return ("FL".to_string(), float_data.to_be_bytes().to_vec());
        }
        dashmap::Entry::Vacant(entry) => {
            let mut float_bytes_vec = payload[0..8].to_vec();
            float_bytes_vec.insert(0, 'F' as u8);

            entry.insert(float_bytes_vec.clone());
            return ("FL".to_string(), float_bytes_vec[1..].to_vec());
        }
    }
}

async fn handle_get_arrow_data(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let payload_str = std::str::from_utf8(&payload).expect("Payload error");
    let payload_query_string = payload_str.to_string();

    if let Some(byte_data) = shared_db.get(&payload_query_string) {
        return ("AR".to_string(), byte_data.to_vec());
    }

    let query: Query = match serde_json::from_str(payload_str) {
        Ok(q) => q,
        Err(_e) => {
            let error_code: u16 = 3;
            return ("ER".to_string(), error_code.to_be_bytes().to_vec());
        }
    };

    let record_batch: Option<RecordBatch>;
    if let Some(record_batch_bytes) = shared_db.get(&query.key) {
        if record_batch_bytes[0] as char != 'A' {
            let error_code: u16 = 4;
            return ("ER".to_string(), error_code.to_be_bytes().to_vec());
        }

        let mut reader = StreamReader::try_new(&record_batch_bytes[1..], None).expect("Read error");
        match reader.next() {
            Some(Ok(rb)) => {
                record_batch = Some(rb);
            },
            Some(Err(_)) => {
                let error_code: u16 = 4;
                return ("ER".to_string(), error_code.to_be_bytes().to_vec());
            },
            None => {
                let error_code: u16 = 4;
                return ("ER".to_string(), error_code.to_be_bytes().to_vec());
            }
        }
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }

    let filtered_record_batch = process_filter(&record_batch.unwrap(), &query.columns, &query.filterlogic, &query.filter);

    let alignment = 64;
    let write_legacy_ipc_format = false;
    let mut write_options: IpcWriteOptions = IpcWriteOptions::try_new(
        alignment, write_legacy_ipc_format, MetadataVersion::V5
    ).unwrap();
    if query.compression_type == "lz4" {
        write_options = write_options.try_with_compression(Some(CompressionType::LZ4_FRAME)).unwrap();
    } else if query.compression_type == "zstd" {
        write_options = write_options.try_with_compression(Some(CompressionType::ZSTD)).unwrap();
    }

    let mut writer = StreamWriter::try_new_with_options(
        Vec::new(), &filtered_record_batch.schema(), write_options
    ).expect("Schema error");

    let _ = writer.write(&filtered_record_batch);
    let _ = writer.finish();
    let buffer: Vec<u8> = writer.into_inner().expect("Buffer error");

    if query.cachetime > 0 {
        shared_db.insert(payload_query_string.clone(), buffer.clone());
        let now = SystemTime::now();
        let duration = Duration::from_millis(query.cachetime);
        timeout_db.insert(payload_query_string, now + duration);
    }
    return ("AR".to_string(), buffer);
}

async fn handle_get_data(payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let get_key = std::str::from_utf8(&payload).expect("Payload error");

    if let Some(bytes_data) = shared_db.get(get_key) {
        let data_type = bytes_data[0] as char;
        if data_type == 'A' {
            return ("AR".to_string(), bytes_data[1..].to_vec());
        } else if data_type == 'B' {
            return ("BY".to_string(), bytes_data[1..].to_vec());
        } else if data_type == 'I' {
            return ("IN".to_string(), bytes_data[1..].to_vec());
        } else if data_type == 'F' {
            return ("FL".to_string(), bytes_data[1..].to_vec());
        } else {
            let error_code: u16 = 5;
            return ("ER".to_string(), error_code.to_be_bytes().to_vec());
        }
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }
}

async fn handle_delete(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let del_key = std::str::from_utf8(&payload).expect("Payload error");

    let _ = timeout_db.remove(del_key);
    if let Some(_) = shared_db.remove(del_key) {
        return ("OK".to_string(), vec![0; 0]);
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }
}

async fn handle_touch(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let cache_time_bytes: [u8; 8] = payload[0..8].try_into().expect("Incorrect length");
    let cache_time_ms = u64::from_be_bytes(cache_time_bytes);

    let key = match std::str::from_utf8(&payload[8..]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    if shared_db.contains_key(&key) {
        let now = SystemTime::now();
        let duration = Duration::from_millis(cache_time_ms);
        timeout_db.insert(key, now + duration);
        return ("OK".to_string(), vec![0; 0]);
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }
}

async fn handle_ttl(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let ttl_key = std::str::from_utf8(&payload).expect("Payload error");

    if let Some(live_until) = timeout_db.get(ttl_key) {
        let now = SystemTime::now();
        match live_until.duration_since(now) {
            Ok(ttl) => {
                let ttl_u64 = ttl.as_millis() as u64;
                return ("TL".to_string(), ttl_u64.to_be_bytes().to_vec());
            }
            Err(_e) => {
                let error_code: u16 = 0;
                return ("ER".to_string(), error_code.to_be_bytes().to_vec());
            }
        }
    } else {
        if shared_db.contains_key(ttl_key) {
            let ttl_u64: u64 = 0;
            return ("TL".to_string(), ttl_u64.to_be_bytes().to_vec());
        } else {
            let error_code: u16 = 2;
            return ("ER".to_string(), error_code.to_be_bytes().to_vec());
        }
    }
}

async fn handle_list_keys(shared_db: SharedDB) -> (String, Vec<u8>) {
    let mut keys_payload_bytes: Vec<u8> = Vec::new();

    for entry in shared_db.iter() {
        let key_bytes = entry.key().as_bytes();
        let _query: Query = match serde_json::from_slice(key_bytes) {
            Ok(_q) => _q,
            Err(_e) => {
                keys_payload_bytes.extend(key_bytes);
                keys_payload_bytes.push(0);
                continue;
            }
        };
    }
    keys_payload_bytes.pop();
    return ("KY".to_string(), keys_payload_bytes);
}

async fn handle_delete_many(timeout_db: TimeoutDB, payload: Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let del_keys_str = std::str::from_utf8(&payload).expect("Payload error");
    let del_keys: Vec<&str> = del_keys_str.split(0 as char).collect();
    let mut count: u16 = 0;

    for key in del_keys {
        let _ = timeout_db.remove(key);
        if let Some(_) = shared_db.remove(key) {
            count += 1;
        }
    }
    return ("DM".to_string(), count.to_be_bytes().to_vec());
}

async fn handle_connection_close() -> (String, Vec<u8>) {
    return ("CC".to_string(), vec![0; 0]);
}

async fn handle_unknown_type() -> (String, Vec<u8>) {
    let error_code: u16 = 1;
    return ("ER".to_string(), error_code.to_be_bytes().to_vec());
}
