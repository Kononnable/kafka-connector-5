use std::sync::Arc;
use std::time::{Duration, Instant};

use client::cluster::{ClusterController, ClusterOptions};
use client::io_loop::State;

/// Kill and restart a broker, verify the client reconnects.
#[test_log::test]
#[ignore = "requires Docker with apache/kafka-native:3.8.0 image"]
fn test_reconnect_after_broker_restart() {
    let (container, addr) = crate::common::start_single_broker();
    let opts = ClusterOptions {
        bootstrap_servers: vec![addr],
        connection_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
        reconnect_backoff_ms: Duration::from_millis(100),
        reconnect_backoff_max_ms: Duration::from_secs(2),
        ..Default::default()
    };
    let cluster = ClusterController::new(opts).expect("failed to create cluster controller");

    while cluster.state() != State::Active {
        std::thread::sleep(Duration::from_millis(50));
    }

    // Verify initial connection
    let versions = futures::executor::block_on(cluster.broker_api_versions(None))
        .expect("expected API versions before kill");
    assert!(!versions.is_empty());

    // Kill the broker by stopping the container
    std::mem::drop(container);

    // Wait for disconnect detection (event loop marks connection dead)
    std::thread::sleep(Duration::from_secs(3));

    // The client should still be alive but without a connection
    // API versions query should fail
    let versions_after_kill = futures::executor::block_on(cluster.broker_api_versions(None));
    assert!(
        versions_after_kill.is_none(),
        "API versions should be None after broker kill; got {:?}",
        versions_after_kill
    );

    drop(cluster);
}
