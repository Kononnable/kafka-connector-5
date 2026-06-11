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

    /// Initial reconnect backoff duration.
    #[derivative(Default(value = "Duration::from_millis(50)"))]
    pub reconnect_backoff_ms: Duration,

    /// Maximum reconnect backoff duration.
    #[derivative(Default(value = "Duration::from_millis(1_000)"))]
    pub reconnect_backoff_max_ms: Duration,
}

impl ClusterOptions {
    pub fn validate(&self) -> Result<(), Vec<ClusterOptionsValidationError>> {
        let mut errors = Vec::new();

        if self.bootstrap_servers.is_empty() {
            errors.push(ClusterOptionsValidationError::NoBootstrapServers);
        }

        for addr in &self.bootstrap_servers {
            if let Err(msg) = validate_bootstrap_addr(addr) {
                errors.push(ClusterOptionsValidationError::InvalidAddress {
                    address: addr.clone(),
                    msg,
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

fn validate_bootstrap_addr(addr: &str) -> Result<(), String> {
    let port_str = addr.rsplit(':').next().ok_or_else(|| {
        format!("missing port in bootstrap address \"{addr}\"")
    })?;
    port_str
        .parse::<u16>()
        .map_err(|_| format!("invalid port in bootstrap address \"{addr}\""))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_ok_with_valid_servers() {
        let opts = ClusterOptions {
            bootstrap_servers: vec!["127.0.0.1:9092".to_string()],
            ..Default::default()
        };
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn validate_ok_with_multiple_valid_servers() {
        let opts = ClusterOptions {
            bootstrap_servers: vec![
                "127.0.0.1:9092".to_string(),
                "127.0.0.1:9093".to_string(),
                "127.0.0.1:9094".to_string(),
            ],
            ..Default::default()
        };
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn validate_ok_with_hostname() {
        let opts = ClusterOptions {
            bootstrap_servers: vec![
                "localhost:9092".to_string(),
                "kafka-broker-1.example.com:9092".to_string(),
            ],
            ..Default::default()
        };
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn validate_fails_with_empty_servers() {
        let opts = ClusterOptions {
            bootstrap_servers: Vec::new(),
            ..Default::default()
        };
        let err = opts.validate().unwrap_err();
        assert_eq!(err.len(), 1);
        assert!(matches!(
            err[0],
            ClusterOptionsValidationError::NoBootstrapServers
        ));
    }

    #[test]
    fn validate_fails_with_missing_port() {
        let opts = ClusterOptions {
            bootstrap_servers: vec!["not-a-valid-address".to_string()],
            ..Default::default()
        };
        let err = opts.validate().unwrap_err();
        assert_eq!(err.len(), 1);
        match &err[0] {
            ClusterOptionsValidationError::InvalidAddress { address, .. } => {
                assert_eq!(address, "not-a-valid-address");
            }
            _ => panic!("expected InvalidAddress"),
        }
    }

    #[test]
    fn validate_fails_with_invalid_port() {
        let opts = ClusterOptions {
            bootstrap_servers: vec!["127.0.0.1:notaport".to_string()],
            ..Default::default()
        };
        let err = opts.validate().unwrap_err();
        assert_eq!(err.len(), 1);
        match &err[0] {
            ClusterOptionsValidationError::InvalidAddress { address, .. } => {
                assert_eq!(address, "127.0.0.1:notaport");
            }
            _ => panic!("expected InvalidAddress"),
        }
    }

    #[test]
    fn validate_fails_with_multiple_errors() {
        let opts = ClusterOptions {
            bootstrap_servers: vec![
                "".to_string(),
                "not-an-address".to_string(),
                "also-invalid:abc".to_string(),
            ],
            ..Default::default()
        };
        let err = opts.validate().unwrap_err();
        assert_eq!(err.len(), 3);
        for e in &err {
            assert!(matches!(e, ClusterOptionsValidationError::InvalidAddress { .. }));
        }
    }
}
