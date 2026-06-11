use std::fmt;
use std::time::{Duration, Instant};

use protocol::traits::ApiVersion;

pub(crate) use protocol::traits::ApiKey;

/// A correlation ID for matching requests to responses within a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CorrelationId(pub i32);

impl CorrelationId {
    pub const fn new(v: i32) -> Self {
        Self(v)
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for CorrelationId {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<CorrelationId> for i32 {
    fn from(id: CorrelationId) -> Self {
        id.0
    }
}

/// A Kafka broker node ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BrokerId(pub i32);

impl BrokerId {
    pub const fn new(v: i32) -> Self {
        Self(v)
    }
}

impl fmt::Display for BrokerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for BrokerId {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<BrokerId> for i32 {
    fn from(id: BrokerId) -> Self {
        id.0
    }
}

/// Identifies a [`RequestHandler`](crate::io_loop::RequestHandler) instance
/// within the event loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RequestHandlerId(pub usize);

impl RequestHandlerId {
    pub const fn new(v: usize) -> Self {
        Self(v)
    }
}

impl fmt::Display for RequestHandlerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<usize> for RequestHandlerId {
    fn from(v: usize) -> Self {
        Self(v)
    }
}

impl From<RequestHandlerId> for usize {
    fn from(id: RequestHandlerId) -> Self {
        id.0
    }
}

/// A request that has been sent but not yet responded to.
#[derive(Debug, Clone)]
pub(crate) struct InflightRequest {
    pub api_key: ApiKey,
    pub version: ApiVersion,
    pub sent_at: Instant,
    pub deadline: Instant,
    pub handler_id: RequestHandlerId,
}

impl InflightRequest {
    pub fn new(
        api_key: ApiKey,
        version: ApiVersion,
        sent_at: Instant,
        timeout: Duration,
        handler_id: RequestHandlerId,
    ) -> Self {
        InflightRequest {
            api_key,
            version,
            sent_at,
            deadline: sent_at + timeout,
            handler_id,
        }
    }
}
