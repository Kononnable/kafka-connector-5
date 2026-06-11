mod common;

use std::time::Duration;

use client::cluster::{ClusterController, ClusterOptions, State};

#[test_log::test]
fn test_bootstrap_single_broker() {
    let (_container, addr) = common::start_single_broker();
    let opts = ClusterOptions {
        bootstrap_servers: vec![addr],
        connection_timeout: Duration::from_secs(30),
        request_timeout: Duration::from_secs(30),
        connection_retry_delay: Duration::from_secs(2),
        ..Default::default()
    };
    let cluster = ClusterController::new(opts).expect("failed to create cluster controller");

    let mut attempts = 0;
    while cluster.state() != State::Active && attempts < 60 {
        std::thread::sleep(Duration::from_millis(500));
        attempts += 1;
    }
    assert_eq!(
        cluster.state(),
        State::Active,
        "cluster did not become active within 30 seconds"
    );
    drop(cluster);
}
