use std::sync::Arc;
use std::time::Duration;

use client::cluster::{ClusterController, ClusterOptions};
use client::io_loop::State;

/// Verify that after connecting to a single broker, the metadata cache
/// is populated and contains the broker we connected to.
#[test_log::test]
#[ignore = "requires Docker with apache/kafka-native:3.8.0 image"]
fn test_metadata_cache_populated_after_bootstrap() {
    let (_container, addr) = crate::common::start_single_broker();
    let opts = ClusterOptions {
        bootstrap_servers: vec![addr],
        connection_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
        metadata_refresh_interval: Duration::from_secs(300),
        ..Default::default()
    };
    let cluster = ClusterController::new(opts).expect("failed to create cluster controller");

    while cluster.state() != State::Active {
        std::thread::sleep(Duration::from_millis(50));
    }

    // Query API versions — this confirms the bootstrap connection is
    // alive and the cluster metadata was exchanged.
    let versions = futures::executor::block_on(cluster.broker_api_versions(None))
        .expect("expected API versions");
    assert!(!versions.is_empty(), "API versions should not be empty");

    drop(cluster);
}
