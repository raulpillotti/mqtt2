use super::mqtt;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

pub struct Tcp {
    _server_thread: std::thread::JoinHandle<()>,
}

impl Tcp {
    pub fn new(mqtt_state: &mqtt::MqttState) -> Self {
        let last_reads = Arc::clone(&mqtt_state.last_reads);
        let handle = std::thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8084").expect("Failed starting tcp server");
            for stream in listener.incoming() {
                let last_reads = Arc::clone(&last_reads);
                if let Ok(stream) = stream {
                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, last_reads) {
                            println!("Handle connection failed: {e}");
                        }
                    });
                }
            }
        });

        Tcp { _server_thread: handle }
    }
}

fn handle_connection(mut stream: TcpStream, last_reads: Arc<RwLock<HashMap<String, Option<String>>>>) -> std::io::Result<()> {
    let client_address = stream.peer_addr().ok();
    println!("Connection from {:?}", client_address);
    let buffer = load_req_into_buffer(&stream)?;
    let topic = find_topic(&buffer);
    println!("topic: {topic}");

    if let Ok(reads) = last_reads.try_read() {
        let entry = reads.get(topic);
        println!("cached: {:?}", reads);
        println!("entry: {:?}", entry);
        let response = match entry {
            Some(Some(val)) => build_response(val),
            None | Some(&None) => "".to_string(),
        };
        println!("response: {response}");
        stream.write_all(response.as_bytes())?;
        stream.flush()?;
    }

    Ok(())
}

fn load_req_into_buffer(stream: &TcpStream) -> std::io::Result<String> {
    let mut reader = BufReader::new(stream);
    let mut buffer = String::new();
    for _ in 0..5 {
        reader.read_line(&mut buffer)?;
    }
    Ok(buffer)
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
