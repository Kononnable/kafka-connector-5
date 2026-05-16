use std::sync::Arc;

use super::ConsumerOptions;
use super::error::ConsumerOptionsValidationError;
use crate::cluster::ClusterController;

pub struct ConsumerController {
    _cluster: Arc<ClusterController>,
    _options: ConsumerOptions,
}

impl ConsumerController {
    pub fn new(
        cluster: Arc<ClusterController>,
        options: ConsumerOptions,
    ) -> Result<Self, Vec<ConsumerOptionsValidationError>> {
        options.validate()?;
        Ok(ConsumerController {
            _cluster: cluster,
            _options: options,
        })
    }
}
