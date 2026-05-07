//! Kafka proxy binary entry point.
//!
//! Starts the proxy, listens for client connections, and forwards to the
//! upstream Kafka broker, rewriting Metadata responses along the way.

use std::net::SocketAddr;
use proxy::{ProxyConfig, ProxyServer};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("proxy=info")),
        )
        .init();

    // Parse command-line arguments or use defaults
    let listen_addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9192".to_string())
        .parse()
        .expect("Usage: proxy [LISTEN_ADDR] [BROKER_ADDR]");

    let broker_addr: SocketAddr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9092".to_string())
        .parse()
        .expect("Usage: proxy [LISTEN_ADDR] [BROKER_ADDR]");

    let config = ProxyConfig::new(listen_addr, broker_addr);
    let server = ProxyServer::new(config);

    tracing::info!(
        "starting proxy on {} -> upstream {}",
        listen_addr,
        broker_addr,
    );

    server.run().await?;

    Ok(())
}
