use std::io::BufRead;
use std::time::Duration;

use testcontainers::core::ContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::GenericImage;

/// Start a single-node Kafka broker using the native GraalVM image.
///
/// Returns the container handle and the bootstrap address string.
pub fn start_single_broker() -> (testcontainers::Container<GenericImage>, String) {
    let image = GenericImage::new("apache/kafka-native", "3.8.0")
        .with_env_var("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw")
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://127.0.0.1:9092")
        .with_ready_conditions(vec![testcontainers::core::WaitFor::message_on_stdout(
            "Kafka Server started",
        )])
        .with_startup_timeout(Duration::from_secs(120));
    let container = image.start().expect("failed to start Kafka container");
    let host_port = container
        .get_host_port_ipv4(ContainerPort::Tcp(9092))
        .expect("failed to get mapped port");
    let addr = format!("127.0.0.1:{host_port}");
    (container, addr)
}

/// Read log lines from a container's stdout.
pub fn container_logs(
    container: &testcontainers::Container<GenericImage>,
) -> Vec<String> {
    let mut stdout = container.stdout(true).expect("failed to get container stdout");
    let mut lines = Vec::new();
    let reader = std::io::BufReader::new(&mut stdout);
    for line in reader.lines() {
        match line {
            Ok(l) => lines.push(l),
            Err(_) => break,
        }
    }
    lines
}
