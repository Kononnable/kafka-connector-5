//! Error types for [`super::ClusterController`].

use crate::broker_controller::BrokerError;
use thiserror::Error;

/// Errors originating from the cluster controller.
#[derive(Debug, Error)]
pub enum ClusterError {
    #[error("broker error: {0}")]
    Broker(#[from] BrokerError),

    #[error("no connected brokers available")]
    NoConnectedBrokers,

    #[error("unknown broker node ID: {0}")]
    UnknownBroker(i32),

    #[error("metadata error: broker returned error code {0}")]
    MetadataError(i16),

    #[error("API key {api_key} version {requested} is not supported by the client (client supports {client_min}-{client_max})")]
    ClientUnsupportedVersion {
        api_key: i16,
        requested: i16,
        client_min: i16,
        client_max: i16,
    },

    #[error("API key {api_key} version {requested} is not supported by the broker (broker supports {broker_min}-{broker_max})")]
    BrokerUnsupportedVersion {
        api_key: i16,
        requested: i16,
        broker_min: i16,
        broker_max: i16,
    },
}
