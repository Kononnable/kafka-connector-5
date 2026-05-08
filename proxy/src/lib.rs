//! Kafka Transparent Proxy
//!
//! A transparent TCP proxy that sits between a Kafka client and a Kafka broker.
//! It understands the Kafka wire protocol and can inspect, log, or modify
//! requests and responses as they pass through.

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


