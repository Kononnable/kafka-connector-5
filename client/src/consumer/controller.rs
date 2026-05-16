use std::sync::Arc;

use crate::cluster::ClusterController;

use super::error::ConsumerOptionsValidationError;
use super::ConsumerOptions;

pub struct ConsumerController {
    cluster: Arc<ClusterController>,
    options: ConsumerOptions,
}

impl ConsumerController {
    pub fn new(
        cluster: Arc<ClusterController>,
        options: ConsumerOptions,
    ) -> Result<Self, Vec<ConsumerOptionsValidationError>> {
        options.validate()?;
        Ok(ConsumerController { cluster, options })
    }
}
