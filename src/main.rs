mod mqtt;
mod tcp;

const MQTT_CONFIG: &'static str = include_str!("../rumqttd.toml");

fn main() {
    let mqtt_context = mqtt::MqttContext::new();
    let _start_broker = mqtt::MqttManager::new(&mqtt_context, MQTT_CONFIG)
        .expect("Failed initializing mqtt broker");
    let _start_tcp_server = tcp::Tcp::new(&mqtt_context);

    loop {
        if let Ok(cached) = mqtt_context.cache.try_read() {
            println!("Cache: {:?}", cached);
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
