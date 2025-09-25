use super::mqtt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

pub struct Tcp {
    _handle: std::thread::JoinHandle<()>,
}

impl Tcp {
    pub fn new(context: &mqtt::MqttContext) -> Self {
        let cache = Arc::clone(&context.cache);
        let handle = std::thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8084").expect("Failed starting tcp server");
            for stream in listener.incoming() {
                let cache = Arc::clone(&cache);
                let _ = std::thread::spawn(move || {
                    if let Ok(mut client_socket) = stream {
                        let client_address = client_socket.peer_addr().ok();
                        println!("Connection from {:?}", client_address);

                        let mut reader = BufReader::new(&client_socket);
                        let mut buffer = String::new();
                        let req_bytes = reader.read_line(&mut buffer);
                        for _ in 0..5 {
                            let _ = reader.read_line(&mut buffer);
                        }

                        match req_bytes {
                            Ok(_read) => {
                                let header_start_byte_idx = buffer.find("topic").unwrap_or_default();
                                let header = &buffer[header_start_byte_idx..];
                                let topic = header
                                    .trim_start_matches("topic: ")
                                    .trim_matches(|c| c == '\r' || c == '\n');

                                if let Ok(cached) = cache.try_read() {
                                    let entry = cached.get(topic);
                                    let response = match entry {
                                        Some(Some(val)) => {
                                            let body = format!("{}", val);
                                            let response = format!(
                                                "HTTP/1.1 200 OK\r\n\
                                                                Content-Type: text/plain; charset=utf-8\r\n\
                                                                Content-Length: {}\r\n\r\n\
                                                                {}",
                                                body.len(),
                                                body
                                            );
                                            response
                                        }
                                        None | Some(&None) => "".to_string(),
                                    };
                                    let _ = client_socket.write_all(response.as_bytes());
                                    let _ = client_socket.flush();
                                }
                            }
                            Err(e) => println!("Error: {e}"),
                        }
                    }
                });
            }
        });

        Tcp { _handle: handle }
    }
}
