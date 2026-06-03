//! Record header structure for Kafka Record v2.
//!
//! Each record can carry zero or more headers (key-value metadata pairs).

use bytes::{Buf, BufMut};

use crate::protocol::serialization::{decode_unsigned_varint, encode_unsigned_varint};

/// A single header within a Kafka Record.
///
/// On-disk format:
/// - headerKeyLength: varint (zig-zag encoded i32)
/// - headerKey: String (UTF-8 bytes, length determined by headerKeyLength)
/// - headerValueLength: varint (zig-zag encoded i32, -1 means null)
/// - headerValue: byte[] (length determined by headerValueLength)
#[derive(Debug, Clone, PartialEq)]
pub struct RecordHeader {
    /// Header key (UTF-8 string).
    pub key: String,
    /// Header value (raw bytes), `None` when value length is -1.
    pub value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Encode this header into `buf`.
    ///
    /// Wire format:
    /// - headerKeyLength: zig-zag varint (byte length of the key string)
    /// - headerKey: UTF-8 bytes
    /// - headerValueLength: zig-zag varint (-1 = null, otherwise byte length)
    /// - headerValue: raw bytes (absent when null)
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        // key length as zig-zag varint
        encode_unsigned_varint(zig_zag_i32(self.key.len() as i32), buf);
        buf.put_slice(self.key.as_bytes());
        // value: -1 for null, otherwise zig-zag of byte length
        match &self.value {
            None => {
                encode_unsigned_varint(1, buf); // zig-zag(-1) = 1
            }
            Some(data) => {
                encode_unsigned_varint(zig_zag_i32(data.len() as i32), buf);
                buf.put_slice(data);
            }
        }
    }

    /// Decode a header from `buf`.
    pub fn decode<B: Buf>(buf: &mut B) -> Self {
        let key_len = zag_zig_i32(&mut *buf) as usize;
        let key_bytes = buf.copy_to_bytes(key_len);
        let key = String::from_utf8(key_bytes.to_vec()).expect("invalid UTF-8 in header key");

        let value_len = zag_zig_i32(&mut *buf);
        let value = if value_len < 0 {
            None
        } else {
            Some(buf.copy_to_bytes(value_len as usize).to_vec())
        };

        Self { key, value }
    }
}

/// Encode `value` as a zig-zag unsigned varint (i32).
#[inline]
pub(crate) fn zig_zag_i32(value: i32) -> u64 {
    ((value << 1) ^ (value >> 31)) as u64
}

/// Encode `value` as a zig-zag unsigned varint (i64).
#[inline]
pub(crate) fn zig_zag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Decode a zig-zag encoded unsigned varint into i32.
#[inline]
pub(crate) fn zag_zig_i32<B: Buf>(buf: &mut B) -> i32 {
    let (unsigned, _) = decode_unsigned_varint(buf).expect("invalid varint");
    ((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32))
}

/// Decode a zig-zag encoded unsigned varint into i64.
#[inline]
pub(crate) fn zag_zig_i64<B: Buf>(buf: &mut B) -> i64 {
    let (unsigned, _) = decode_unsigned_varint(buf).expect("invalid varint");
    ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_roundtrip_simple() {
        let h = RecordHeader {
            key: "content-type".into(),
            value: Some(b"application/json".to_vec()),
        };
        let mut buf = BytesMut::new();
        h.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = RecordHeader::decode(&mut read);
        assert_eq!(h, decoded);
    }

    #[test]
    fn test_roundtrip_null_value() {
        let h = RecordHeader {
            key: "trace-id".into(),
            value: None,
        };
        let mut buf = BytesMut::new();
        h.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = RecordHeader::decode(&mut read);
        assert_eq!(h, decoded);
    }

    #[test]
    fn test_roundtrip_empty() {
        let h = RecordHeader {
            key: String::new(),
            value: None,
        };
        let mut buf = BytesMut::new();
        h.encode(&mut buf);

        let mut read: &[u8] = &buf;
        let decoded = RecordHeader::decode(&mut read);
        assert_eq!(h, decoded);
    }
}
