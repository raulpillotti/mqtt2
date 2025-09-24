use core::error::Error;
use rumqttd::{local::LinkTx, Broker, Config, Notification};
use std::{
    sync::{Arc, Mutex},
    thread,
};

pub struct MqttContext {
    pub recent_topic: Arc<Mutex<Option<String>>>,
    pub recent_data: Arc<Mutex<Option<String>>>,
}

impl MqttContext {
    pub fn new() -> Self {
        Self {
            recent_topic: Arc::new(Mutex::new(None)),
            recent_data: Arc::new(Mutex::new(None)),
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

        let recent_topic = Arc::clone(&context.recent_topic);
        let recent_data = Arc::clone(&context.recent_data);

        let task_handler = std::thread::spawn(move || loop {
            let received = match rx.recv() {
                Ok(Some(value)) => Some(value),
                Ok(None) => continue,
                Err(e) => {
                    println!("Error receveing packet: {e}");
                    None
                }
            };

            if let Some(rec) = received {
                match rec {
                    Notification::Forward(forward) => {
                        let topic = String::from_utf8_lossy(&forward.publish.topic).to_string();
                        let payload = String::from_utf8_lossy(&forward.publish.payload).to_string();
                        println!("MQTT Topic = {:?}, Payload = {}", &topic, &payload);

                        if let Ok(mut recent) = recent_topic.lock() {
                            *recent = Some(topic);
                        }
                        if let Ok(mut recent) = recent_data.lock() {
                            *recent = Some(payload);
                        }
                    }
                    v => {
                        println!("MQTT {v:?}");
                    }
                }
            }
        });

        Ok(MqttManager {
            _broker_thread: Arc::new(broker_handle),
            _task_handler: Arc::new(task_handler),
            _tx: Arc::new(tx),
        })
    }
}
