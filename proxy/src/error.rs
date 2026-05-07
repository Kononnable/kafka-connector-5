//! Error types for the Kafka proxy.

use std::io;
use thiserror::Error;

/// Errors that can occur during proxy operation.
#[derive(Debug, Error)]
pub enum ProxyError {
    /// I/O error from TCP sockets.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Protocol serialization/deserialization error.
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::traits::SerializationError),

    /// Error establishing or maintaining the upstream connection.
    #[error("upstream connection error: {0}")]
    Upstream(String),

    /// Error from the client connection.
    #[error("client connection error: {0}")]
    Client(String),

    /// Invalid or unsupported correlation ID mapping.
    #[error("correlation id mismatch: expected {expected}, got {got}")]
    CorrelationIdMismatch { expected: i32, got: i32 },
}
