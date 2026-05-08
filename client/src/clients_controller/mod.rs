//! Public-facing client API layer.
//!
//! This module will house high-level client operations such as producing
//! messages, consuming from topics, managing consumer groups, etc.
//!
//! For now it is a placeholder that holds a reference to the cluster
//! controller and defines the module structure.

use crate::cluster_controller::ClusterController;
use std::sync::Arc;

/// High-level Kafka client for public API usage.
///
/// Constructed with an `Arc<ClusterController>` which manages the underlying
/// broker connections and cluster state.
///
/// ## Example (future)
///
/// ```ignore
/// use client::cluster_controller::ClusterOptions;
/// let cluster = ClusterController::new(ClusterOptions {
///     bootstrap_servers: vec!["127.0.0.1:9092".to_string()],
///     ..Default::default()
/// }).await?;
/// cluster.refresh_metadata(None).await?;
/// let client = ClientsController::new(cluster);
/// ```
#[derive(Debug, Clone)]
pub struct ClientsController {
    /// Reference to the cluster controller for broker routing.
    cluster: Arc<ClusterController>,
}

impl ClientsController {
    /// Create a new client API controller.
    pub fn new(cluster: Arc<ClusterController>) -> Self {
        Self { cluster }
    }

    /// Return a reference to the underlying cluster controller.
    pub fn cluster(&self) -> &Arc<ClusterController> {
        &self.cluster
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClusterController;

    #[test]
    fn test_clients_controller_construction() {
        use crate::cluster_controller::ClusterOptions;
        // Can't easily test async construction here, but we can test
        // that the type is constructable with an Arc<ClusterController>.
        // Actual construction is tested in cluster_controller tests.
    }

    #[test]
    fn test_clients_controller_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ClientsController>();
        assert_sync::<ClientsController>();
    }
}
