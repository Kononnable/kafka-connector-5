//! RecordBatch structure for Kafka message format v2 (magic byte 2).
//!
//! A record batch is the unit of data exchange between Kafka clients and
//! brokers. It contains a header followed by zero or more records.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};

use super::attributes::RecordBatchAttributes;
use super::record::Record;

/// A Kafka RecordBatch (magic v2).
///
/// On-disk layout:
///
/// | Field                | Type    | Offset | Description                                      | Stored |
/// |----------------------|---------|--------|--------------------------------------------------|--------|
/// | baseOffset           | int64   | 0      | First offset of this batch                       | yes    |
/// | batchLength          | int32   | 8      | Total size from partitionLeaderEpoch to end      | **no** |
/// | partitionLeaderEpoch | int32   | 12     | Epoch of the leader at write time                | yes    |
/// | magic                | int8    | 16     | Format version (must be 2)                       | **no** |
/// | crc                  | uint32  | 17     | CRC-32C from attributes to end of batch          | **no** |
/// | attributes           | int16   | 21     | Bitflags (compression, timestamp type, txn, etc.)| yes    |
/// | lastOffsetDelta      | int32   | 23     | Delta from baseOffset to last record's offset    | **no** |
/// | baseTimestamp        | int64   | 27     | Timestamp of the first record (Unix epoch ms)    | yes    |
/// | maxTimestamp         | int64   | 35     | Maximum timestamp in the batch                   | **no** |
/// | producerId           | int64   | 43     | Producer ID for idempotent/transactional writes  | yes    |
/// | producerEpoch        | int16   | 51     | Producer epoch                                   | yes    |
/// | baseSequence         | int32   | 53     | First sequence number for idempotent writes      | yes    |
/// | recordsCount         | int32   | 57     | Number of records in this batch                  | **no** |
/// | records              | [Record]| 61     | The record data                                  | yes    |
///
/// **Wire-only fields** (not stored in the struct):
/// - `batchLength` — computed on [`encode`], validated on [`decode`] for CRC bounds.
/// - `magic` — always `2`; [`decode`] panics if not.
/// - `crc` — CRC-32C over attributes-to-end; recomputed on [`encode`], verified
///   on [`decode`] (panics on mismatch).
/// - `lastOffsetDelta` — derived from `records.last().offset_delta` on [`encode`].
/// - `maxTimestamp` — derived from `base_timestamp + max(records.timestamp_delta)` on [`encode`].
/// - `recordsCount` — derived from `records.len()` on [`encode`].
///
/// The CRC covers everything from `attributes` through to the end of the
/// `records` field. `partitionLeaderEpoch` is excluded from CRC to avoid
/// recomputation on the broker.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    /// First offset of this batch (broker-assigned).
    pub base_offset: i64,
    /// Epoch of the partition leader at write time.
    pub partition_leader_epoch: i32,
    /// Batch attributes (bitflags).
    pub attributes: RecordBatchAttributes,
    /// Timestamp of the first record.
    ///
    /// Internally stored as [`SystemTime`]; converted to/from Unix epoch
    /// milliseconds on the wire.
    pub base_timestamp: SystemTime,
    /// Producer ID for idempotent/transactional writes (-1 if not used).
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// First sequence number for idempotent writes (-1 if not used).
    pub base_sequence: i32,
    /// The records in this batch.
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Encode this batch into `buf`.
    ///
    /// On encode the CRC and batchLength are computed after all data has been
    /// written, then patched back into their reserved positions in the buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        // [0..8]   baseOffset
        buf.put_i64(self.base_offset);

        // [8..12]  batchLength – placeholder, filled later
        let batch_len_pos = buf.len();
        buf.put_i32(0);

        // [12..16] partitionLeaderEpoch
        buf.put_i32(self.partition_leader_epoch);

        // [16]     magic — always 2
        buf.put_i8(2);

        // [17..21] crc – placeholder, filled later
        let crc_pos = buf.len();
        buf.put_u32(0);

        // -- CRC-covered region starts here (offset 21 from batch start) --

        // [21..23] attributes
        buf.put_i16(self.attributes.into());

        // [23..27] lastOffsetDelta — derived from records
        buf.put_i32(self.records.last().map_or(0, |r| r.offset_delta));

        // [27..35] baseTimestamp
        buf.put_i64(ts_to_millis(self.base_timestamp));

        // [35..43] maxTimestamp — derived from records
        let max_ts_delta = self
            .records
            .iter()
            .map(|r| r.timestamp_delta)
            .max()
            .unwrap_or(0);
        buf.put_i64(ts_to_millis(self.base_timestamp) + max_ts_delta);

        // [43..51] producerId
        buf.put_i64(self.producer_id);

        // [51..53] producerEpoch
        buf.put_i16(self.producer_epoch);

        // [53..57] baseSequence
        buf.put_i32(self.base_sequence);

        // [57..61] recordsCount — derived from records.len()
        buf.put_i32(self.records.len() as i32);

        // records
        for record in &self.records {
            record.encode(buf);
        }

        // -- patch batchLength and CRC --
        // batchLength is total bytes from partitionLeaderEpoch (byte 12) to end.
        let total_len = buf.len();
        let batch_len = (total_len - 12) as i32;

        let crc_data = &buf[crc_pos + 4..]; // from attributes (byte 21) to end
        let crc = crc32c::crc32c(crc_data);

        buf[batch_len_pos..batch_len_pos + 4].copy_from_slice(&batch_len.to_be_bytes());
        buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());
    }

    /// Decode a record batch from `buf`, performing CRC verification.
    ///
    /// Panics if the magic byte is not 2 or if the CRC does not match.
    pub fn decode<B: Buf>(buf: &mut B) -> Self {
        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32();
        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();
        assert_eq!(magic, 2, "unsupported magic value {magic}");
        let stored_crc = buf.get_u32();

        // CRC covers from attributes (byte 21) to end of batch.
        // batchLength runs from partitionLeaderEpoch (byte 12) to end,
        // so the CRC-covered region is (batchLength - 9) bytes.
        let crc_len = (batch_length - 9) as usize;
        let crc_data = buf.chunk();
        assert!(crc_data.len() >= crc_len, "batch truncated");
        let computed_crc = crc32c::crc32c(&crc_data[..crc_len]);
        assert_eq!(
            computed_crc, stored_crc,
            "CRC mismatch: computed {computed_crc:#010x}, stored {stored_crc:#010x}"
        );

        let attributes = RecordBatchAttributes::from(buf.get_i16());
        let _last_offset_delta = buf.get_i32();
        let base_timestamp = millis_to_ts(buf.get_i64());
        let _max_timestamp = buf.get_i64();
        let producer_id = buf.get_i64();
        let producer_epoch = buf.get_i16();
        let base_sequence = buf.get_i32();
        let records_count = buf.get_i32();

        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
            records.push(Record::decode(&mut *buf));
        }

        Self {
            base_offset,
            partition_leader_epoch,
            attributes,
            base_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }

    /// Return `true` if this batch is a control batch (transaction marker).
    pub fn is_control_batch(&self) -> bool {
        self.attributes.is_control_batch()
    }

    /// Return `true` if this batch is transactional.
    pub fn is_transactional(&self) -> bool {
        self.attributes.is_transactional()
    }

    /// Return the compression type as a raw u8.
    pub fn compression_type(&self) -> u8 {
        self.attributes.compression_type()
    }
}

fn ts_to_millis(ts: SystemTime) -> i64 {
    ts.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

fn millis_to_ts(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis((-ms) as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use bytes::{BufMut, BytesMut};

    use super::super::attributes::RecordBatchAttributes;
    use super::super::record::Record;
    use super::super::record_header::RecordHeader;
    use super::RecordBatch;

    #[test]
    fn test_roundtrip_basic() {
        let batch = RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: RecordBatchAttributes::empty(),
            base_timestamp: UNIX_EPOCH + Duration::from_millis(1000),
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(b"data".to_vec()),
                headers: vec![],
            }],
        };

        let mut buf = BytesMut::new();
        batch.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = RecordBatch::decode(&mut read);

        assert_eq!(decoded.base_offset, batch.base_offset);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].value, Some(b"data".to_vec()));
    }

    #[test]
    fn test_roundtrip_multiple_records() {
        let records = vec![
            Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(b"a".to_vec()),
                value: Some(b"1".to_vec()),
                headers: vec![],
            },
            Record {
                attributes: 0,
                timestamp_delta: 100,
                offset_delta: 1,
                key: None,
                value: Some(b"2".to_vec()),
                headers: vec![],
            },
        ];

        let batch = RecordBatch {
            base_offset: 42,
            partition_leader_epoch: 5,
            attributes: RecordBatchAttributes::TRANSACTIONAL,
            base_timestamp: UNIX_EPOCH + Duration::from_millis(5000),
            producer_id: 12345,
            producer_epoch: 0,
            base_sequence: 0,
            records,
        };

        let mut buf = BytesMut::new();
        batch.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = RecordBatch::decode(&mut read);

        assert_eq!(decoded.base_offset, 42);
        assert_eq!(decoded.partition_leader_epoch, 5);
        assert!(decoded.attributes.is_transactional());
        assert_eq!(decoded.producer_id, 12345);
        assert_eq!(decoded.records.len(), 2);
    }

    #[test]
    #[should_panic(expected = "unsupported magic value")]
    fn test_reject_bad_magic() {
        let mut buf = BytesMut::new();
        buf.put_i64(0); // baseOffset
        buf.put_i32(0); // batchLength
        buf.put_i32(0); // partitionLeaderEpoch
        buf.put_i8(1); // magic = 1 (unsupported)
        buf.put_u32(0); // crc
        let mut read: &[u8] = &buf;
        RecordBatch::decode(&mut read);
    }

    #[test]
    #[should_panic(expected = "CRC mismatch")]
    fn test_reject_bad_crc() {
        let mut buf = BytesMut::new();
        buf.put_i64(0); // baseOffset
        buf.put_i32(26); // batchLength
        buf.put_i32(0); // partitionLeaderEpoch
        buf.put_i8(2); // magic
        buf.put_u32(0xdeadbeef); // bogus CRC
        buf.put_i16(0); // attributes
        buf.put_i32(0); // lastOffsetDelta
        buf.put_i64(0); // baseTimestamp
        buf.put_i64(0); // maxTimestamp
        buf.put_i64(-1); // producerId
        buf.put_i16(-1); // producerEpoch
        buf.put_i32(-1); // baseSequence
        buf.put_i32(0); // recordsCount
        let mut read: &[u8] = &buf;
        RecordBatch::decode(&mut read);
    }

    #[test]
    fn test_decode_real_packet() {
        let packet: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00,
            0x00, 0x00, 0x02, 0xf0, 0x15, 0x91, 0x41, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x9b, 0x76, 0xce, 0xb2, 0x5d, 0x00, 0x00, 0x01, 0x9b, 0x76, 0xce, 0xb2,
            0x5d, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00, 0x01, 0x44, 0x00, 0x00, 0x00, 0x12, 0x4e, 0x34, 0x52, 0x64,
            0x6d, 0x57, 0x42, 0x35, 0x30, 0x0a, 0x51, 0x66, 0x44, 0x33, 0x38, 0x02, 0x10, 0x61,
            0x70, 0x70, 0x2e, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x74, 0x65, 0x73, 0x74,
        ];
        let mut read = packet;
        let decoded = RecordBatch::decode(&mut read);

        let expected = RecordBatch {
            base_offset: 3,
            partition_leader_epoch: 0,
            attributes: RecordBatchAttributes::empty(),
            base_timestamp: UNIX_EPOCH + Duration::from_millis(1767224816221),
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(b"N4RdmWB50".to_vec()),
                value: Some(b"QfD38".to_vec()),
                headers: vec![RecordHeader {
                    key: "app.name".into(),
                    value: Some(b"test".to_vec()),
                }],
            }],
        };
        assert_eq!(decoded, expected);

        // Round-trip: encode back and compare to original packet
        let mut encoded = BytesMut::new();
        decoded.encode(&mut encoded);
        assert_eq!(&encoded[..], packet);
    }
}
