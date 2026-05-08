//! Example: create a ClusterController, connect to the 3-node cluster,
//! fetch metadata, and verify all brokers are discovered and connected.

use client::cluster_controller::{ClusterController, ClusterOptions};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Connect to the 3-node KRaft cluster.
    let cluster = ClusterController::new(ClusterOptions {
        bootstrap_servers: vec![
            "127.0.0.1:19092".to_string(),
            "127.0.0.1:29092".to_string(),
            "127.0.0.1:39092".to_string(),
        ],
        ..Default::default()
    })
    .await?;

    println!("✓ Cluster controller created");

    // Check state
    let count = cluster.broker_count().await;
    let connected = cluster.connected_broker_count().await;
    let ctrl_id = cluster.controller_node_id().await;
    let cluster_id = cluster.cluster_id().await;

    println!("  broker_count:      {count}");
    println!("  connected_brokers: {connected}");
    println!("  controller_id:     {ctrl_id}");
    println!("  cluster_id:        {:?}", cluster_id);

    if connected < 3 {
        eprintln!("⚠  Expected 3 connected brokers, got {connected}");
    } else {
        println!("✓ All 3 brokers connected");
    }

    // List connected broker IDs
    let ids = cluster.connected_broker_ids().await;
    println!("  broker node IDs: {:?}", ids);

    // Try fetching metadata again to confirm ongoing connectivity
    let meta = cluster.refresh_metadata(None).await?;
    println!("✓ Metadata refreshed — {} broker(s) in cluster", meta.brokers.len());

    Ok(())
}
