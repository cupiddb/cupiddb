use std::sync::Arc;
use std::time::{SystemTime, Duration};

use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{StreamWriter, IpcWriteOptions};
use arrow::ipc::CompressionType;
use arrow::ipc::gen::Schema::MetadataVersion;
use dashmap::DashMap;
use serde::Deserialize;
use glob_match::glob_match;

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
    pub value_int: Option<i128>,
    pub value_flt: Option<f64>,
    pub value_bol: Option<bool>,
    pub value_str: Option<String>,
}

pub fn handle_frame(message_type: &String, payload: &Vec<u8>,
                    timeout_db: TimeoutDB, shared_db: SharedDB) -> (String, Vec<u8>) {
    match message_type.as_str() {
        "SD" => handle_set_data(timeout_db, payload, shared_db),
        "II" => handle_increment_integer(payload, shared_db),
        "IF" => handle_increment_float(payload, shared_db),
        "GA" => handle_get_arrow_data(timeout_db, payload, shared_db),
        "GD" => handle_get_data(payload, shared_db),
        "DL" => handle_delete(timeout_db, payload, shared_db),
        "TH" => handle_touch(timeout_db, payload, shared_db),
        "TL" => handle_ttl(timeout_db, payload, shared_db),
        "HK" => handle_has_key(payload, shared_db),
        "LS" => handle_list_keys(payload, shared_db),
        "DM" => handle_delete_many(timeout_db, payload, shared_db),
        "FU" => handle_flush(timeout_db, shared_db),
        "WP" => handle_wrong_protocol(),
        "CC" => handle_connection_close(),
        _ => handle_unknown_type(),
    }
}

fn handle_set_data(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let cache_time_bytes: [u8; 8] = payload[0..8].try_into().expect("Incorrect length");
    let cache_time_ms = u64::from_be_bytes(cache_time_bytes);
    let is_add = payload[8] != 0;
    let key_index_until = (u16::from_be_bytes([payload[9], payload[10]]) + 11) as usize;
    let key = match std::str::from_utf8(&payload[11..key_index_until]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    if is_add && shared_db.contains_key(&key) {
        return ("NA".to_string(), vec![0; 0]);
    }

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

fn handle_increment_integer(payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_increment_float(payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_get_arrow_data(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_get_data(payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_delete(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let del_key = std::str::from_utf8(&payload).expect("Payload error");

    let _ = timeout_db.remove(del_key);
    if let Some(_) = shared_db.remove(del_key) {
        return ("OK".to_string(), vec![0; 0]);
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }
}

fn handle_touch(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let cache_time_bytes: [u8; 8] = payload[0..8].try_into().expect("Incorrect length");
    let cache_time_ms = u64::from_be_bytes(cache_time_bytes);

    let key = match std::str::from_utf8(&payload[8..]) {
        Ok(valid_str) => { valid_str.to_string() },
        Err(_) => { panic!("Invalid") },
    };

    if shared_db.contains_key(&key) {
        if cache_time_ms > 0 {
            let now = SystemTime::now();
            let duration = Duration::from_millis(cache_time_ms);
            timeout_db.insert(key, now + duration);
        } else {
            let _ = timeout_db.remove(&key);
        }
        return ("OK".to_string(), vec![0; 0]);
    } else {
        let error_code: u16 = 2;
        return ("ER".to_string(), error_code.to_be_bytes().to_vec());
    }
}

fn handle_ttl(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_has_key(payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let key = std::str::from_utf8(&payload).expect("Payload error");

    if shared_db.contains_key(key) {
        return ("OK".to_string(), vec![1; 1]);
    } else {
        return ("OK".to_string(), vec![0; 1]);
    }
}

fn handle_list_keys(payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
    let mut keys_payload_bytes: Vec<u8> = Vec::new();
    let pattern = std::str::from_utf8(payload).unwrap();

    for entry in shared_db.iter() {
        let key_bytes = entry.key().as_bytes();
        let _query: Query = match serde_json::from_slice(key_bytes) {
            Ok(_q) => _q,
            Err(_e) => {
                if payload.len() == 0 {
                    keys_payload_bytes.extend(key_bytes);
                    keys_payload_bytes.push(0);
                } else if glob_match(pattern, std::str::from_utf8(key_bytes).unwrap()) {
                    keys_payload_bytes.extend(key_bytes);
                    keys_payload_bytes.push(0);
                }
                continue;
            }
        };
    }
    keys_payload_bytes.pop();
    return ("KY".to_string(), keys_payload_bytes);
}

fn handle_delete_many(timeout_db: TimeoutDB, payload: &Vec<u8>, shared_db: SharedDB) -> (String, Vec<u8>) {
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

fn handle_flush(timeout_db: TimeoutDB, shared_db: SharedDB) -> (String, Vec<u8>) {
    timeout_db.clear();
    shared_db.clear();
    return ("FU".to_string(), vec![0; 0]);
}

fn handle_wrong_protocol() -> (String, Vec<u8>) {
    let error_code: u16 = 6;
    return ("ER".to_string(), error_code.to_be_bytes().to_vec());
}

fn handle_connection_close() -> (String, Vec<u8>) {
    return ("CC".to_string(), vec![0; 0]);
}

fn handle_unknown_type() -> (String, Vec<u8>) {
    let error_code: u16 = 1;
    return ("ER".to_string(), error_code.to_be_bytes().to_vec());
}
