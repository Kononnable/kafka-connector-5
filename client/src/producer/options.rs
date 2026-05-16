use super::error::ProducerOptionsValidationError;

#[derive(Debug)]
pub struct ProducerOptions;

impl ProducerOptions {
    pub fn validate(&self) -> Result<(), Vec<ProducerOptionsValidationError>> {
        // TODO: add producer-specific validation
        Ok(())
    }
}
