use std::sync::Arc;
use std::time::Duration;

use client::cluster::{ClusterController, ClusterOptions};
use client::io_loop::State;
use futures_timer::Delay;

/// Verify broker_api_versions contains known API keys after bootstrap.
#[test_log::test]
#[ignore = "requires Docker with apache/kafka-native:3.8.0 image"]
fn test_api_versions_contains_expected_keys() {
    let (_container, addr) = crate::common::start_single_broker();
    let opts = ClusterOptions {
        bootstrap_servers: vec![addr],
        connection_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let cluster = ClusterController::new(opts).expect("failed to create cluster controller");

    // Wait for Active
    while cluster.state() != State::Active {
        std::thread::sleep(Duration::from_millis(50));
    }

    // Query API versions synchronously using a simple async runtime
    let versions = futures::executor::block_on(cluster.broker_api_versions(None))
        .expect("expected API versions");

    // Known API keys that should always be present
    assert!(versions.contains_key(&3), "MetadataRequest (api_key=3) should be present");
    assert!(versions.contains_key(&0), "ProduceRequest (api_key=0) should be present");
    assert!(versions.contains_key(&1), "FetchRequest (api_key=1) should be present");
    assert!(versions.contains_key(&18), "ApiVersionsRequest (api_key=18) should be present");

    // Verify version ranges are sensible
    let metadata = &versions[&3];
    assert!(metadata.max_version >= 9, "MetadataRequest max_version should be >= 9 (flexible)");
    assert!(metadata.min_version >= 0, "MetadataRequest min_version should be >= 0");

    drop(cluster);
}
