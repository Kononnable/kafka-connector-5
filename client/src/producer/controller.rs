use std::sync::Arc;

use super::ProducerOptions;
use super::error::ProducerOptionsValidationError;
use crate::cluster::ClusterController;

pub struct ProducerController {
    _cluster: Arc<ClusterController>,
    _options: ProducerOptions,
}

impl ProducerController {
    pub fn new(
        cluster: Arc<ClusterController>,
        options: ProducerOptions,
    ) -> Result<Self, Vec<ProducerOptionsValidationError>> {
        options.validate()?;
        Ok(ProducerController {
            _cluster: cluster,
            _options: options,
        })
    }
}
