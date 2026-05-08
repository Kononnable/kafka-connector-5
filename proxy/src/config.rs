//! Proxy configuration.

use std::collections::HashMap;
use std::net::SocketAddr;

/// Configuration for the Kafka proxy server.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address the proxy listens on for incoming Kafka client connections.
    pub listen_addr: SocketAddr,
    /// Address of the upstream Kafka broker to forward requests to.
    pub broker_addr: SocketAddr,
    /// Mapping from broker port → proxy port for MetadataResponse rewriting.
    /// When the proxy sees a broker with host == proxy_host and a port in this
    /// map, it rewrites the port to the mapped proxy port.
    pub port_map: HashMap<u16, u16>,
}

impl ProxyConfig {
    /// Create a new proxy configuration.
    ///
    /// `port_map` maps broker ports to proxy ports (e.g. 9092→9192, 9093→9193).
    /// If empty, a single mapping from broker port to listen port is used.
    pub fn new(
        listen_addr: SocketAddr,
        broker_addr: SocketAddr,
        mut port_map: HashMap<u16, u16>,
    ) -> Self {
        if port_map.is_empty() {
            port_map.insert(broker_addr.port(), listen_addr.port());
        }
        Self {
            listen_addr,
            broker_addr,
            port_map,
        }
    }

    /// The host string used to match brokers in MetadataResponse for rewriting.
    pub fn proxy_host(&self) -> String {
        self.listen_addr.ip().to_string()
    }

    /// Map a broker port to its proxy port. Returns `None` if no mapping.
    /// Accepts `i32` (Kafka wire format) and returns `i32` for byte patching.
    pub fn proxy_port_for(&self, broker_port: i32) -> Option<i32> {
        let bp = u16::try_from(broker_port).ok()?;
        self.port_map.get(&bp).copied().map(i32::from)
    }

    /// Parse a port map string like "9092:9192,9093:9193,9094:9194".
    pub fn parse_port_map(s: &str) -> Result<HashMap<u16, u16>, String> {
        let mut map = HashMap::new();
        for pair in s.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("invalid port mapping: {pair}, expected broker_port:proxy_port"));
            }
            let broker_port: u16 = parts[0].trim().parse()
                .map_err(|_| format!("invalid broker port: {}", parts[0]))?;
            let proxy_port: u16 = parts[1].trim().parse()
                .map_err(|_| format!("invalid proxy port: {}", parts[1]))?;
            map.insert(broker_port, proxy_port);
        }
        if map.is_empty() {
            return Err("port map must have at least one entry".into());
        }
        Ok(map)
    }
}
