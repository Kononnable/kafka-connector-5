//! Kafka Protocol Parser for Rust
//!
//! This library provides functionality to parse Kafka protocol JSON definitions 
//! into Rust structs for building Kafka clients.

pub mod generator;

pub use generator::structs::{MessageStruct, Field, MessageType};