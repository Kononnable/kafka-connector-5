use super::metadata::BrokerInfo;

pub(crate) struct Connection;

pub(crate) struct InflightRequest;

pub(crate) enum ConnectionState {
    Connecting,
    ApiVersions,
    Ready(BrokerInfo),
}
