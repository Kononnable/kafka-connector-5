use std::sync::Arc;
use std::time::Duration;

use client::cluster::{ClusterController, ClusterOptions};
use client::io_loop::State;

/// Connect to a single-broker cluster and verify the lifecycle reaches Active.
#[test_log::test]
#[ignore = "requires Docker with apache/kafka-native:3.8.0 image"]
fn test_bootstrap_single_broker() {
    let (_container, addr) = crate::common::start_single_broker();
    let opts = ClusterOptions {
        bootstrap_servers: vec![addr],
        connection_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let cluster = ClusterController::new(opts).expect("failed to create cluster controller");

    let mut attempts = 0;
    while cluster.state() != State::Active && attempts < 50 {
        std::thread::sleep(Duration::from_millis(100));
        attempts += 1;
    }
    assert_eq!(
        cluster.state(),
        State::Active,
        "cluster did not become active within 5 seconds"
    );
    drop(cluster);
}
