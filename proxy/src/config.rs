//! Proxy configuration.

use std::net::SocketAddr;

/// Configuration for the Kafka proxy server.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address the proxy listens on for incoming Kafka client connections.
    /// This address is injected into Metadata responses so clients connect
    /// through the proxy instead of directly to brokers.
    pub listen_addr: SocketAddr,
    /// Address of the upstream Kafka broker to forward requests to.
    pub broker_addr: SocketAddr,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9092".parse().unwrap(),
            broker_addr: "127.0.0.1:9093".parse().unwrap(),
        }
    }
}

impl ProxyConfig {
    /// Create a new proxy configuration.
    pub fn new(listen_addr: SocketAddr, broker_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            broker_addr,
        }
    }

    /// The host string to inject into metadata responses (e.g. "127.0.0.1").
    pub fn proxy_host(&self) -> String {
        self.listen_addr.ip().to_string()
    }

    /// The port to inject into metadata responses.
    pub fn proxy_port(&self) -> i32 {
        self.listen_addr.port() as i32
    }
}
