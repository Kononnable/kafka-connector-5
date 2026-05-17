//! Kafka Record Batch (message format v2, magic byte 2).
//!
//! This module provides types for the Kafka record batch wire format.
//! Only magic value 2 is supported; compression parsing is available via
//! [`RecordBatchAttributes`] but decompression is not yet implemented.

pub mod attributes;
pub mod batch;
pub mod control_record;
pub mod record;
pub mod record_header;

pub use attributes::RecordBatchAttributes;
pub use batch::RecordBatch;
pub use control_record::{ControlRecordKey, ControlRecordType, ControlRecordValue};
pub use record::Record;
pub use record_header::RecordHeader;
