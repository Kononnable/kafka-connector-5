use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;

use testcontainers::core::IntoContainerPort;
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

/// Set `DOCKER_HOST` to the Podman socket if podman is available and Docker isn't.
fn ensure_docker_host() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        if std::env::var("DOCKER_HOST").is_ok() {
            return;
        }
        if Path::new("/run/user/1001/podman/podman.sock").exists() {
            unsafe {
                std::env::set_var("DOCKER_HOST", "unix:///run/user/1001/podman/podman.sock");
            }
            tracing::info!(
                "using podman socket at DOCKER_HOST=unix:///run/user/1001/podman/podman.sock"
            );
        }
    });
}

/// Start a single-node Kafka broker using the native GraalVM image.
///
/// Returns the container handle and the bootstrap address string.
pub fn start_single_broker() -> (testcontainers::Container<GenericImage>, String) {
    ensure_docker_host();
    let image = GenericImage::new("apache/kafka-native", "3.8.0")
        .with_exposed_port(9092.tcp())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
            "Kafka Server started",
        ))
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
        .with_startup_timeout(Duration::from_secs(120));
    let container = image.start().expect("failed to start Kafka container");
    let host_port = container
        .get_host_port_ipv4(9092.tcp())
        .expect("failed to get mapped port");
    let addr = format!("127.0.0.1:{host_port}");
    (container, addr)
}
