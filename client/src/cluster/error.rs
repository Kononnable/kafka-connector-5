#[derive(Debug, thiserror::Error)]
pub enum ClusterOptionsValidationError {
    #[error("at least one bootstrap server is required")]
    NoBootstrapServers,
    #[error("invalid bootstrap address \"{address}\": {msg}")]
    InvalidAddress {
        address: String,
        msg: String,
    },
}
