//! Kafka Transparent Proxy
//!
//! A transparent TCP proxy that sits between a Kafka client and a Kafka broker.
//! It understands the Kafka wire protocol and can inspect, log, or modify
//! requests and responses as they pass through.

use std::fmt;

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

pub use crate::config::ProxyConfig;
pub use crate::error::ProxyError;
pub use crate::server::ProxyServer;

// ---------------------------------------------------------------------------
// Sub-modules
// ---------------------------------------------------------------------------

pub mod config;
pub mod error;
pub mod frame;
pub mod server;
pub mod tracker;

// ---------------------------------------------------------------------------
// Direction
// ---------------------------------------------------------------------------

/// Direction of a proxied message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From client to broker.
    Request,
    /// From broker to client.
    Response,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Request => write!(f, "REQUEST"),
            Direction::Response => write!(f, "RESPONSE"),
        }
    }
}
