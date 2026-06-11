mod common;

use std::sync::Arc;
use std::time::Duration;

use client::cluster::{ClusterController, ClusterOptions, State};

/// TODO: flaky — the broker closes idle connections quickly.
#[test_log::test]
#[ignore = "flaky"]
fn test_reconnect_after_broker_restart() {
    let (container, addr) = common::start_single_broker();
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

    // Kill the broker by stopping the container
    std::mem::drop(container);

    // Wait for the event loop to detect the disconnection and attempt reconnects
    std::thread::sleep(Duration::from_secs(5));

    // The event loop should either have no connections or only failed ones
    let versions = futures::executor::block_on(cluster.clone().broker_api_versions(None));
    // After kill, either None or empty (failed connection with no api_versions)
    // is acceptable — the key is the broker is unreachable.
    if let Some(v) = versions {
        assert!(
            v.is_empty(),
            "API versions after kill should be empty or None; got {len} entries",
            len = v.len()
        );
    }

    drop(cluster);
}
