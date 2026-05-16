use super::error::ConsumerOptionsValidationError;

#[derive(Debug)]
pub struct ConsumerOptions;

impl ConsumerOptions {
    pub fn validate(&self) -> Result<(), Vec<ConsumerOptionsValidationError>> {
        // TODO: add consumer-specific validation
        Ok(())
    }
}
