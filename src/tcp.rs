use super::mqtt;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub struct Tcp {
    _server_thread: std::thread::JoinHandle<()>,
}

impl Tcp {
    pub fn new(context: &mqtt::MqttContext) -> Self {
        let cache = Arc::clone(&context.cache);
        let handle = std::thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8084").expect("Failed starting tcp server");
            for stream in listener.incoming() {
                let cache = Arc::clone(&cache);
                if let Ok(client_socket) = stream {
                    let _ = std::thread::spawn(move || {
                        handle_connection(client_socket, cache);
                    });
                }
            }
        });

        Tcp { _server_thread: handle }
    }
}

fn handle_connection(mut client_socket: TcpStream, cache: Arc<RwLock<HashMap<String, Option<String>>>>) -> () {
    let client_address = client_socket.peer_addr().ok();
    println!("Connection from {:?}", client_address);
    let buffer = load_req_into_buffer(&client_socket);
    let topic = find_topic(buffer.as_str());
    println!("topic: {topic}");

    if let Ok(cached) = cache.try_read() {
        let entry = cached.get(topic);
        let response = match entry {
            Some(Some(val)) => build_response(val),
            None | Some(&None) => "".to_string(),
        };
        let _ = client_socket.write_all(response.as_bytes());
        let _ = client_socket.flush();
    }
}

fn load_req_into_buffer(client_socket: &TcpStream) -> String {
    let mut reader = BufReader::new(client_socket);
    let mut buffer = String::new();
    for _ in 0..5 {
        let _ = reader.read_line(&mut buffer);
    }
    buffer
}

fn find_topic<'a>(buffer: &'a str) -> &'a str {
    let header_start_byte_idx = buffer.find("topic").unwrap_or_default();
    let header = &buffer[header_start_byte_idx..];
    let topic = header
        .trim_start_matches("topic: ")
        .trim_matches(|c| c == '\r' || c == '\n');
    topic
}

fn build_response(value: &str) -> String {
    let body = format!("{}", value);
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
