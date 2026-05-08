//! Cluster-level options for [`super::ClusterController`].

/// Cluster-level options passed to [`super::ClusterController::new`].
#[derive(Debug, Clone, derivative::Derivative)]
#[derivative(Default)]
pub struct ClusterOptions {
    /// List of `host:port` bootstrap servers to connect to.
    ///
    /// At least one must be reachable.
    #[derivative(Default(value = "vec![]"))]
    pub bootstrap_servers: Vec<String>,

    /// Client software name sent in ApiVersionsRequest.
    #[derivative(Default(value = "env!(\"CARGO_PKG_NAME\").to_owned()"))]
    pub client_software_name: String,

    /// Client software version sent in ApiVersionsRequest.
    #[derivative(Default(value = "env!(\"CARGO_PKG_VERSION\").to_owned()"))]
    pub client_software_version: String,
}
