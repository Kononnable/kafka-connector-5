//! Kafka Client Library
//!
//! A three-layer Kafka client:
//!
//! - **BrokerController** — manages a TCP connection to a single broker, tracks
//!   in-flight requests via correlation IDs, and optionally performs API version
//!   negotiation.
//!
//! - **ClusterController** — cluster-wide state (metadata, broker list).  Creates
//!   and drops `BrokerController` instances as per `MetadataResponse`.  Provides
//!   request routing to specific or random connected brokers with automatic API
//!   version selection.
//!
//! - **ClientsController** — public-facing API layer (module placeholder for now).

pub mod broker_controller;
pub mod cluster_controller;
pub mod clients_controller;

pub use broker_controller::BrokerController;
pub use cluster_controller::ClusterController;
pub use clients_controller::ClientsController;
