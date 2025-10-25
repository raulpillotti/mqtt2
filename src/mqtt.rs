use core::error::Error;
use rumqttd::{local::LinkTx, Broker, Config, Notification};
use std::{
    collections::HashMap, sync::{Arc, RwLock}, thread
};

pub struct MqttState {
    pub last_reads: Arc<RwLock<HashMap<String, Option<String>>>>,
}

impl MqttState {
    pub fn new() -> Self {
        Self {
            last_reads: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
pub struct MqttServer {
    _broker_thread: Arc<std::thread::JoinHandle<()>>,
    _task_handler_thread: Arc<std::thread::JoinHandle<()>>,
    _tx: Arc<LinkTx>,
}

impl MqttServer {
    pub fn new(state: &MqttState, config: &str) -> Result<Self, Box<dyn Error>> {
        let config_build = config::Config::builder()
            .add_source(config::File::from_str(config, config::FileFormat::Toml))
            .build()?;

        let config: Config = config_build.try_deserialize()?;
        let mut broker = Broker::new(config);

        let (mut tx, mut rx) = broker.link("singlenode")?;
        tx.subscribe("#")?;

        let broker_handle = thread::spawn(move || {
            broker.start().expect("Error starting broker: {e}");
        });

        let reads = Arc::clone(&state.last_reads);
        let task_handle = std::thread::spawn(move || loop {
            let received = match rx.recv() {
                Ok(Some(value)) => Some(value),
                Ok(None) => continue,
                Err(e) => {
                    println!("Error receveing packet: {e}");
                    None
                }
            };

            if let Some(rec) = received {
                handle_packet(rec, Arc::clone(&reads));
            }
        });

        Ok(MqttServer {
            _broker_thread: Arc::new(broker_handle),
            _task_handler_thread: Arc::new(task_handle),
            _tx: Arc::new(tx),
        })
    }
}

fn handle_packet(rec: Notification, last_reads: Arc<RwLock<HashMap<String, Option<String>>>>) {
    match rec {
        Notification::Forward(forward) => {
            let topic = String::from_utf8_lossy(&forward.publish.topic).to_string();
            let payload = String::from_utf8_lossy(&forward.publish.payload).to_string();
            println!("MQTT Topic = {:?}, Payload = {}", &topic, &payload);

            if let Ok(mut reads) = last_reads.write() {
               reads.insert(topic, Some(payload));
            }
        }
        _ => {}
    }
}
