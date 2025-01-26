use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PROTOCOL_VERSION: char = 'B';
const HEADER_SIZE: usize = 11;

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: socket,
        }
    }

    pub async fn read_frame(&mut self) -> (String, Vec<u8>) {
        let mut header_buffer = [0; HEADER_SIZE];
        let mut payload_length_buffer = [0; 8];
        let packet_length: u64;
        let message_type: String;

        match self.stream.read_exact(&mut header_buffer).await {
            Ok(_) => {
                payload_length_buffer.clone_from_slice(&header_buffer[3..HEADER_SIZE]);
                packet_length = u64::from_be_bytes(payload_length_buffer);
                if header_buffer[0] as char == PROTOCOL_VERSION {
                    message_type = match String::from_utf8((&header_buffer[1..3]).to_vec()) {
                        Ok(mt) => { mt },
                        Err(_) => { panic!("Invalid string") },
                    }
                } else {
                    message_type = "WP".to_string();
                }
            },
            Err(e) => {
                tracing::debug!("Failed to read from socket: {}", e);
                message_type = "CC".to_string();
                packet_length = 0;
            },
        }

        let mut payload = vec![0; (packet_length) as usize];
        if packet_length > 0 {
            match self.stream.read_exact(&mut payload).await {
                Ok(_) => {},
                Err(e) => {
                    tracing::error!("Failed to read payload: {}", e);
                },
            }
        }
        return (message_type, payload);
    }

    pub async fn write_frame(&mut self, message_type: String, payload: Vec<u8>) {
        let header = PROTOCOL_VERSION.to_string() + message_type.as_str();
        let payload_length = payload.len() as u64;

        let mut header_buffer = header.into_bytes();
        header_buffer.extend(payload_length.to_be_bytes());
        match self.stream.write_all(&header_buffer).await {
            Ok(_) => {},
            Err(_) => {},
        }

        if payload_length > 0 {
            match self.stream.write_all(&payload).await {
                Ok(_) => {},
                Err(_) => {},
            }
        }
    }
}
