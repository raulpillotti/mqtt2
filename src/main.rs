mod mqtt;
mod tcp;

const MQTT_CONFIG: &'static str = include_str!("../rumqttd.toml");

fn main() {
    let mqtt_state = mqtt::MqttState::new();
    let _start_broker = mqtt::MqttServer::new(&mqtt_state, MQTT_CONFIG)
        .expect("Failed initializing mqtt broker");
    let _start_tcp_server = tcp::Tcp::new(&mqtt_state);

    loop {
        if let Ok(cached) = mqtt_state.last_reads.try_read() {
            println!("Cache: {:?}", cached);
        }
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
