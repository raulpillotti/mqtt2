use core::error::Error;
use rumqttd::{local::LinkTx, Broker, Config, Notification};
use std::{
    collections::HashMap, sync::{Arc, RwLock}, thread
};

pub struct MqttContext {
    pub cache: Arc<RwLock<HashMap<String, Option<String>>>>,
}

impl MqttContext {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
pub struct MqttManager {
    _broker_thread: Arc<std::thread::JoinHandle<()>>,
    _task_handler: Arc<std::thread::JoinHandle<()>>,
    _tx: Arc<LinkTx>,
}

impl MqttManager {
    pub fn new(context: &MqttContext, config: &str) -> Result<Self, Box<dyn Error>> {
        let config_build = config::Config::builder()
            .add_source(config::File::from_str(config, config::FileFormat::Toml))
            .build()?;

        let config: Config = config_build.try_deserialize()?;

        let mut broker = Broker::new(config);

        let (mut tx, mut rx) = broker.link("singlenode")?;
        tx.subscribe("#")?;

        let broker_handle = thread::spawn(move || {
            if let Err(e) = broker.start() {
                println!("Error starting broker: {e}");
            }
        });

        let cache = Arc::clone(&context.cache);
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
                handle_packet(rec, Arc::clone(&cache));
            }
        });

        Ok(MqttManager {
            _broker_thread: Arc::new(broker_handle),
            _task_handler: Arc::new(task_handle),
            _tx: Arc::new(tx),
        })
    }
}

fn handle_packet(rec: Notification, cache: Arc<RwLock<HashMap<String, Option<String>>>>) {
    match rec {
        Notification::Forward(forward) => {
            let topic = String::from_utf8_lossy(&forward.publish.topic).to_string();
            let payload = String::from_utf8_lossy(&forward.publish.payload).to_string();
            println!("MQTT Topic = {:?}, Payload = {}", &topic, &payload);

            if let Ok(mut cache) = cache.write() {
               cache.insert(topic, Some(payload));
            }
        }
        v => {
            println!("MQTT {v:?}");
        }
    }
}
