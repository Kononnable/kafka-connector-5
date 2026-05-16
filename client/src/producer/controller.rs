use std::sync::Arc;

use crate::cluster::ClusterController;

use super::error::ProducerOptionsValidationError;
use super::ProducerOptions;

pub struct ProducerController {
    cluster: Arc<ClusterController>,
    options: ProducerOptions,
}

impl ProducerController {
    pub fn new(
        cluster: Arc<ClusterController>,
        options: ProducerOptions,
    ) -> Result<Self, Vec<ProducerOptionsValidationError>> {
        options.validate()?;
        Ok(ProducerController { cluster, options })
    }
}
