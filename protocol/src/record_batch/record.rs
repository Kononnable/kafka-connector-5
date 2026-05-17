//! Record structure for Kafka Record v2 (magic byte 2).
//!
//! Each record is a self-describing unit within a RecordBatch.
//! Records use varint/varlong encoding for variable-length fields.

use bytes::{Buf, BufMut, BytesMut};

use super::record_header::{RecordHeader, zag_zig_i32, zag_zig_i64, zig_zag_i32, zig_zag_i64};
use crate::protocol::serialization::encode_unsigned_varint;

/// A single Kafka Record (magic v2 format).
///
/// On-disk format (all variable-length fields use zig-zag varint encoding):
/// - length: varint (total byte length of the record, excluding the length field itself)
/// - attributes: int8 (currently all bits unused)
/// - timestampDelta: varlong (difference from batch's baseTimestamp)
/// - offsetDelta: varint (difference from batch's baseOffset)
/// - keyLength: varint (-1 means null key)
/// - key: byte[] (length determined by keyLength)
/// - valueLength: varint (-1 means null value)
/// - value: byte[] (length determined by valueLength)
/// - headersCount: varint
/// - headers: RecordHeader[] (count determined by headersCount)
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// Record attributes (int8, all bits currently unused).
    pub attributes: i8,
    /// Timestamp delta from the batch's baseTimestamp.
    pub timestamp_delta: i64,
    /// Offset delta from the batch's baseOffset.
    pub offset_delta: i32,
    /// Record key (raw bytes). `None` when key length is -1.
    pub key: Option<Vec<u8>>,
    /// Record value (raw bytes). `None` when value length is -1.
    pub value: Option<Vec<u8>>,
    /// Record headers.
    pub headers: Vec<RecordHeader>,
}

impl Record {
    /// Encode this record into `buf`.
    ///
    /// The body is serialized into a temporary buffer first so that the
    /// leading `length` varint can be computed accurately.
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        // Serialize body into temp buffer
        let mut body = BytesMut::new();

        // attributes: int8
        body.put_i8(self.attributes);
        // timestampDelta: varlong (zig-zag)
        encode_unsigned_varint(zig_zag_i64(self.timestamp_delta), &mut body);
        // offsetDelta: varint (zig-zag)
        encode_unsigned_varint(zig_zag_i32(self.offset_delta), &mut body);
        // key
        match &self.key {
            None => {
                encode_unsigned_varint(1, &mut body); // zig-zag(-1) = 1
            }
            Some(k) => {
                encode_unsigned_varint(zig_zag_i32(k.len() as i32), &mut body);
                body.put_slice(k);
            }
        }
        // value
        match &self.value {
            None => {
                encode_unsigned_varint(1, &mut body);
            }
            Some(v) => {
                encode_unsigned_varint(zig_zag_i32(v.len() as i32), &mut body);
                body.put_slice(v);
            }
        }
        // headersCount (always >= 0)
        encode_unsigned_varint(zig_zag_i32(self.headers.len() as i32), &mut body);
        for header in &self.headers {
            header.encode(&mut body);
        }

        // Write length (zig-zag of body size) then body
        encode_unsigned_varint(zig_zag_i32(body.len() as i32), buf);
        buf.put_slice(&body);
    }

    /// Decode a record from `buf`.
    pub fn decode<B: Buf>(buf: &mut B) -> Self {
        // Record body length
        let body_len = zag_zig_i32(&mut *buf);
        let start_remaining = buf.remaining();

        let attributes = buf.get_i8();
        let timestamp_delta = zag_zig_i64(&mut *buf);
        let offset_delta = zag_zig_i32(&mut *buf);

        let key_len = zag_zig_i32(&mut *buf);
        let key = if key_len < 0 {
            None
        } else {
            let mut k = vec![0u8; key_len as usize];
            buf.copy_to_slice(&mut k);
            Some(k)
        };

        let value_len = zag_zig_i32(&mut *buf);
        let value = if value_len < 0 {
            None
        } else {
            let mut v = vec![0u8; value_len as usize];
            buf.copy_to_slice(&mut v);
            Some(v)
        };

        let headers_count = zag_zig_i32(&mut *buf);
        let mut headers = Vec::with_capacity(headers_count as usize);
        for _ in 0..headers_count {
            headers.push(RecordHeader::decode(&mut *buf));
        }

        // Consume any remaining bytes to match advertised body_len
        let consumed = start_remaining - buf.remaining();
        let expected = body_len as usize;
        if consumed < expected {
            buf.advance(expected - consumed);
        }

        Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::super::record_header::RecordHeader;
    use super::Record;

    #[test]
    fn test_roundtrip_no_headers() {
        let r = Record {
            attributes: 0,
            timestamp_delta: 12345,
            offset_delta: 0,
            key: Some(b"my-key".to_vec()),
            value: Some(b"hello".to_vec()),
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = Record::decode(&mut read);
        assert_eq!(r, decoded);
    }

    #[test]
    fn test_roundtrip_null_key_value() {
        let r = Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 5,
            key: None,
            value: None,
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = Record::decode(&mut read);
        assert_eq!(r, decoded);
    }

    #[test]
    fn test_roundtrip_with_headers() {
        let r = Record {
            attributes: 1,
            timestamp_delta: 999,
            offset_delta: 42,
            key: None,
            value: Some(vec![0xde, 0xad]),
            headers: vec![
                RecordHeader {
                    key: "k1".into(),
                    value: Some(b"v1".to_vec()),
                },
                RecordHeader {
                    key: "k2".into(),
                    value: None,
                },
            ],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = Record::decode(&mut read);
        assert_eq!(r, decoded);
    }

    #[test]
    fn test_roundtrip_negative_deltas() {
        let r = Record {
            attributes: 0,
            timestamp_delta: -100,
            offset_delta: -5,
            key: Some(b"neg".to_vec()),
            value: None,
            headers: vec![],
        };
        let mut buf = BytesMut::new();
        r.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = Record::decode(&mut read);
        assert_eq!(r, decoded);
    }
}
