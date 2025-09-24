use super::mqtt;
use std::io::Write;
use std::net::TcpListener;
use std::sync::Arc;

pub struct Tcp {
    _handle: std::thread::JoinHandle<()>,
}

impl Tcp {
    pub fn new(context: &mqtt::MqttContext) -> Self {
        let recent_data = Arc::clone(&context.recent_data);

        let handle = std::thread::spawn(move || {
            let listener = TcpListener::bind("127.0.0.1:8084").expect("Failed starting tcp server");
            for stream in listener.incoming() {
                if let Ok(mut client_socket) = stream {
                    let client_address = client_socket.peer_addr().ok();
                    println!("Connection from {:?}", client_address);

                    if let Ok(recent) = recent_data.try_read() {
                        if let Some(ref data) = *recent {
                            let body = format!("{}", data);
                            let response = format!(
                                "HTTP/1.1 200 OK\r\n\
                                            Content-Type: text/plain; charset=utf-8\r\n\
                                            Content-Length: {}\r\n\r\n\
                                            {}",
                                body.len(),
                                body
                            );
                            let _ = client_socket.write_all(response.as_bytes());
                            let _ = client_socket.flush();
                        }
                    }
                }
            }
        });

        Tcp { _handle: handle }
    }
}
