//! Bitflags for the `attributes` field in a RecordBatch.
//!
//! The attributes field is a 16-bit integer with the following layout:
//!
//! | Bits    | Meaning                                                  |
//! |---------|----------------------------------------------------------|
//! | 0–2     | Compression type: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd |
//! | 3       | Timestamp type: 0=CreateTime, 1=LogAppendTime            |
//! | 4       | Is transactional (0 = not transactional)                 |
//! | 5       | Is control batch (0 = not a control batch)               |
//! | 6       | Has delete horizon ms (baseTimestamp is the delete horizon) |
//! | 7–15    | Unused                                                    |

use bitflags::bitflags;

bitflags! {
    /// Record batch attributes as a bitflag type.
    ///
    /// Provides named accessors for each sub-field so callers can inspect
    /// individual properties without manually masking.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct RecordBatchAttributes: u16 {
        /// No compression.
        const COMPRESSION_NONE   = 0b000_0000_0000_0000;
        /// Gzip compression.
        const COMPRESSION_GZIP   = 0b000_0000_0000_0001;
        /// Snappy compression.
        const COMPRESSION_SNAPPY = 0b000_0000_0000_0010;
        /// LZ4 compression.
        const COMPRESSION_LZ4    = 0b000_0000_0000_0011;
        /// Zstandard compression.
        const COMPRESSION_ZSTD   = 0b000_0000_0000_0100;
        /// Mask isolating the compression bits (0–2).
        const COMPRESSION_MASK   = 0b000_0000_0000_0111;

        /// Timestamp type: CreateTime (bit 3 = 0).
        const TIMESTAMP_CREATE_TIME    = 0b000_0000_0000_0000;
        /// Timestamp type: LogAppendTime (bit 3 = 1).
        const TIMESTAMP_LOG_APPEND_TIME = 0b000_0000_0000_1000;
        /// Mask isolating the timestamp type bit.
        const TIMESTAMP_TYPE_MASK      = 0b000_0000_0000_1000;

        /// Transactional flag (bit 4).
        const TRANSACTIONAL      = 0b000_0000_0001_0000;
        /// Control batch flag (bit 5).
        const CONTROL_BATCH      = 0b000_0000_0010_0000;
        /// Delete horizon MS flag (bit 6).
        const HAS_DELETE_HORIZON_MS = 0b000_0000_0100_0000;
    }
}

impl RecordBatchAttributes {
    /// Return the compression type as a raw u8 (0–7).
    pub fn compression_type(self) -> u8 {
        (self.bits() & Self::COMPRESSION_MASK.bits()) as u8
    }

    /// Return `true` if the timestamp type is LogAppendTime.
    pub fn is_log_append_time(self) -> bool {
        self.contains(Self::TIMESTAMP_LOG_APPEND_TIME)
    }

    /// Return `true` if the batch is transactional.
    pub fn is_transactional(self) -> bool {
        self.contains(Self::TRANSACTIONAL)
    }

    /// Return `true` if the batch is a control batch.
    pub fn is_control_batch(self) -> bool {
        self.contains(Self::CONTROL_BATCH)
    }

    /// Return `true` if the batch has a delete horizon timestamp.
    pub fn has_delete_horizon_ms(self) -> bool {
        self.contains(Self::HAS_DELETE_HORIZON_MS)
    }
}

impl From<i16> for RecordBatchAttributes {
    fn from(raw: i16) -> Self {
        Self::from_bits_retain(raw as u16)
    }
}

impl From<RecordBatchAttributes> for i16 {
    fn from(attrs: RecordBatchAttributes) -> Self {
        attrs.bits() as i16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_none() {
        let attrs = RecordBatchAttributes::from(0i16);
        assert_eq!(attrs.compression_type(), 0);
    }

    #[test]
    fn test_compression_gzip() {
        let attrs = RecordBatchAttributes::from(1i16);
        assert_eq!(attrs.compression_type(), 1);
    }

    #[test]
    fn test_timestamp_log_append_time() {
        let attrs = RecordBatchAttributes::from(0b1000i16);
        assert!(attrs.is_log_append_time());
        assert!(!attrs.is_transactional());
        assert!(!attrs.is_control_batch());
    }

    #[test]
    fn test_transactional_flag() {
        let attrs = RecordBatchAttributes::from(0b1_0000i16);
        assert!(attrs.is_transactional());
        assert!(!attrs.is_log_append_time());
    }

    #[test]
    fn test_control_batch() {
        let attrs = RecordBatchAttributes::from(0b10_0000i16);
        assert!(attrs.is_control_batch());
    }

    #[test]
    fn test_delete_horizon() {
        let attrs = RecordBatchAttributes::from(0b100_0000i16);
        assert!(attrs.has_delete_horizon_ms());
    }

    #[test]
    fn test_combined_flags() {
        // transactional | control | gzip
        let raw = 0b11_0001i16;
        let attrs = RecordBatchAttributes::from(raw);
        assert_eq!(attrs.compression_type(), 1);
        assert!(attrs.is_transactional());
        assert!(attrs.is_control_batch());
    }

    #[test]
    fn test_roundtrip_i16() {
        let raw: i16 = 0b11_1001;
        let attrs = RecordBatchAttributes::from(raw);
        let back: i16 = attrs.into();
        assert_eq!(raw, back);
    }
}
