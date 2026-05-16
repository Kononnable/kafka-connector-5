//! Kafka Protocol Parser for Rust
//!
//! This library provides functionality to parse Kafka protocol JSON definitions
//! into Rust structs for building Kafka clients.

pub mod error;
pub mod generated;
pub mod generator;
pub mod protocol;
pub mod traits;

pub use generator::structs::{ErrorCode, Field, FieldDefault, MessageStruct, MessageType};
pub use traits::*;
