//! Kafka proxy binary entry point.
//!
//! Usage:
//!   proxy [LISTEN_ADDR] [BROKER_ADDR] [PORT_MAP]
//!
//! PORT_MAP is optional, format: "broker_port:proxy_port,..."
//! Example for a 3-broker cluster:
//!   proxy 127.0.0.1:9192 127.0.0.1:9092 "9092:9192,9093:9193,9094:9194"

use proxy::{ProxyConfig, ProxyServer};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("proxy=info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();

    let listen_addr: SocketAddr = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:9192".to_string())
        .parse()
        .expect("Usage: proxy [LISTEN_ADDR] [BROKER_ADDR] [PORT_MAP]");

    let broker_addr: SocketAddr = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:9092".to_string())
        .parse()
        .expect("Usage: proxy [LISTEN_ADDR] [BROKER_ADDR] [PORT_MAP]");

    let port_map: HashMap<u16, u16> = if let Some(pm) = args.get(3) {
        ProxyConfig::parse_port_map(pm).expect("PORT_MAP format: broker_port:proxy_port,...")
    } else {
        HashMap::new()
    };

    let config = ProxyConfig::new(listen_addr, broker_addr, port_map);

    tracing::info!(
        "starting proxy on {} -> upstream {} (port map: {:?})",
        config.listen_addr,
        config.broker_addr,
        config.port_map,
    );

    let server = ProxyServer::new(config);

    server.run().await?;

    Ok(())
}
