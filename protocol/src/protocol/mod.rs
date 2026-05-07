//! Kafka wire protocol types and serialization.
//!
//! This module provides the traits and implementations for encoding/decoding
//! data in Kafka's binary wire protocol format.

pub mod serialization;
pub mod types;

pub use serialization::*;
pub use types::*;
