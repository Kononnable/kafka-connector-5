use std::net::SocketAddr;
use std::time::Duration;

use super::error::ClusterOptionsValidationError;

#[derive(Debug, Clone, derivative::Derivative)]
#[derivative(Default)]
pub struct ClusterOptions {
    pub bootstrap_servers: Vec<String>,

    /// Logical client name sent to the broker (defaults to crate name).
    #[derivative(Default(value = r#"String::from(env!("CARGO_CRATE_NAME"))"#))]
    pub client_name: String,

    /// Maximum time to wait for a TCP connection to be established.
    #[derivative(Default(value = "Duration::from_secs(30)"))]
    pub connection_timeout: Duration,

    /// Maximum time to wait for a request/response round-trip.
    #[derivative(Default(value = "Duration::from_secs(30)"))]
    pub request_timeout: Duration,

    /// Delay between connection retry attempts.
    #[derivative(Default(value = "Duration::from_millis(1_000)"))]
    pub connection_retry_delay: Duration,

    /// Maximum time between periodic metadata refreshes.
    #[derivative(Default(value = "Duration::from_secs(300)"))]
    pub metadata_refresh_interval: Duration,
}

impl ClusterOptions {
    pub fn validate(&self) -> Result<(), Vec<ClusterOptionsValidationError>> {
        let mut errors = Vec::new();

        if self.bootstrap_servers.is_empty() {
            errors.push(ClusterOptionsValidationError::NoBootstrapServers);
        }

        for addr in &self.bootstrap_servers {
            if let Err(e) = addr.parse::<SocketAddr>() {
                errors.push(ClusterOptionsValidationError::InvalidAddress {
                    address: addr.clone(),
                    source: e,
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
