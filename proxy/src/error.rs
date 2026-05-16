//! Error types for the Kafka proxy.

use std::io;

use thiserror::Error;

/// Errors that can occur during proxy operation.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// I/O error from TCP sockets.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Error establishing or maintaining the upstream connection.
    #[error("upstream connection error: {0}")]
    Upstream(String),
}
