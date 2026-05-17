//! Control batch record types.
//!
//! Control batches are used by the transaction protocol to mark
//! transactional boundaries (commit/abort). They contain a single
//! record whose key follows a fixed schema.

/// The key of a control record.
///
/// On-disk format (after the record's keyLength):
/// - version: int16 (current version is 0)
/// - type: int16 (0 = abort marker, 1 = commit marker)
#[derive(Debug, Clone, PartialEq)]
pub struct ControlRecordKey {
    /// Control record version (currently 0).
    pub version: i16,
    /// Control record type.
    pub control_type: ControlRecordType,
}

/// Types of control record markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRecordType {
    /// Transaction abort marker (type = 0).
    Abort,
    /// Transaction commit marker (type = 1).
    Commit,
}

impl ControlRecordType {
    /// Return the raw int16 wire value for this control type.
    pub fn as_i16(self) -> i16 {
        match self {
            ControlRecordType::Abort => 0,
            ControlRecordType::Commit => 1,
        }
    }

    /// Construct a `ControlRecordType` from its raw int16 wire value.
    ///
    /// Returns `None` for unknown values.
    pub fn from_i16(raw: i16) -> Option<Self> {
        match raw {
            0 => Some(ControlRecordType::Abort),
            1 => Some(ControlRecordType::Commit),
            _ => None,
        }
    }
}

/// A control record's value (opaque to clients, type-dependent).
///
/// The value schema depends on the control type. Currently the value
/// is treated as opaque bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct ControlRecordValue {
    /// Raw opaque bytes of the control record value.
    pub data: Vec<u8>,
}
