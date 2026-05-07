//! In-flight request tracking for latency measurement.
//!
//! Each request sent to the broker is tracked by its correlation ID along
//! with a timestamp. When the matching response arrives, we compute the
//! round-trip latency and log it.

use crate::frame::{InFlightRequest, ParsedRequestHeader};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Tracks in-flight Kafka requests by correlation ID.
#[derive(Debug, Clone)]
pub struct RequestTracker {
    /// Map from correlation_id -> request metadata + timestamp.
    pending: HashMap<i32, InFlightRequest>,
}

impl RequestTracker {
    /// Create a new empty tracker.
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Record a request that is being forwarded to the broker.
    /// Returns the number of currently tracked in-flight requests.
    pub fn track_request(&mut self, header: &ParsedRequestHeader) -> usize {
        let inflight = InFlightRequest {
            correlation_id: header.correlation_id,
            api_key: header.api_key,
            api_version: header.api_version,
            client_id: header.client_id.clone(),
            sent_at: Instant::now(),
        };
        self.pending.insert(header.correlation_id, inflight);
        self.pending.len()
    }

    /// Complete a tracked request when its response arrives.
    /// Returns the latency if a matching in-flight request was found.
    pub fn complete_response(&mut self, correlation_id: i32) -> Option<RequestCompletion> {
        let inflight = self.pending.remove(&correlation_id)?;
        let latency = inflight.sent_at.elapsed();
        Some(RequestCompletion {
            correlation_id,
            api_key: inflight.api_key,
            api_version: inflight.api_version,
            client_id: inflight.client_id,
            latency,
        })
    }

    /// Number of requests currently in-flight.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Remove all timed-out entries (optional, for cleanup).
    pub fn evict_stale(&mut self, timeout: Duration) {
        self.pending.retain(|_, v| v.sent_at.elapsed() < timeout);
    }
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of matching a response to a tracked request.
#[derive(Debug, Clone)]
pub struct RequestCompletion {
    pub correlation_id: i32,
    pub api_key: i16,
    pub api_version: i16,
    pub client_id: String,
    pub latency: Duration,
}
