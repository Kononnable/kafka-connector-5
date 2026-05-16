use std::net::AddrParseError;

#[derive(Debug, thiserror::Error)]
pub enum ClusterOptionsValidationError {
    #[error("at least one bootstrap server is required")]
    NoBootstrapServers,
    #[error("invalid bootstrap address \"{address}\": {source}")]
    InvalidAddress {
        address: String,
        source: AddrParseError,
    },
}
