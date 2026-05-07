//! Traits and implementations for Kafka's binary wire protocol.
//!
//! Kafka encodes all data on the wire in a consistent format:
//!
//! | Type             | Wire representation                                    |
//! |------------------|--------------------------------------------------------|
//! | `int8` / `bool`  | 1 byte                                                 |
//! | `int16`          | 2 bytes, big-endian                                    |
//! | `int32` / `uint32`| 4 bytes, big-endian                                   |
//! | `int64`          | 8 bytes, big-endian                                    |
//! | `varint`         | unsigned varint (zig-zag encoded i32)                  |
//! | `varlong`        | unsigned varint (zig-zag encoded i64)                  |
//! | `string`         | 2-byte length (int16) + UTF-8 bytes, -1 ⇒ null         |
//! | `nullable_string`| same as string                                         |
//! | `bytes`          | 4-byte length (int32) + raw bytes, -1 ⇒ null           |
//! | `nullable_bytes` | same as bytes                                          |
//! | `array`          | 4-byte length (int32) + N elements, -1 ⇒ null          |
//! | `uuid`           | 16 raw bytes                                           |

use bytes::{Buf, BufMut};
use thiserror::Error as DeriveError;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur during encoding.
#[derive(Debug, Clone, DeriveError)]
pub enum EncodeError {
    /// The value is too large to fit in the required field width.
    #[error("value too large: {message}")]
    ValueTooLarge { message: String },
}

/// Errors that can occur during decoding.
#[derive(Debug, Clone, DeriveError)]
pub enum DecodeError {
    /// The buffer ran out of bytes before the value could be fully read.
    #[error("insufficient bytes in buffer")]
    InsufficientBytes,
    /// The encoded length is negative but the type does not support null.
    #[error("unexpected null value")]
    UnexpectedNull,
    /// The encoded length is invalid (e.g. negative length for a non-nullable type).
    #[error("invalid length: {message}")]
    InvalidLength { message: String },
    /// The string data is not valid UTF-8.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    /// A generic protocol error.
    #[error("protocol error: {message}")]
    Protocol { message: String },
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(_: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8
    }
}

// ---------------------------------------------------------------------------
// Helper: unsigned varint encoding (used internally by varint / varlong)
// ---------------------------------------------------------------------------

/// Encode `value` as an unsigned variable-length integer.
/// Returns the number of bytes written.
fn encode_unsigned_varint<B: BufMut>(mut value: u64, buf: &mut B) -> usize {
    let start = buf.remaining_mut();
    loop {
        if value < 0x80 {
            buf.put_u8(value as u8);
            break;
        } else {
            buf.put_u8((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
    }
    start - buf.remaining_mut()
}

/// Decode an unsigned variable-length integer.
/// Returns `(value, bytes_consumed)`.
fn decode_unsigned_varint<B: Buf>(buf: &mut B) -> Result<(u64, usize), DecodeError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut consumed: usize = 0;
    loop {
        if !buf.has_remaining() {
            return Err(DecodeError::InsufficientBytes);
        }
        let byte = buf.get_u8();
        consumed += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }
        shift += 7;
        if shift >= 64 {
            return Err(DecodeError::Protocol {
                message: "varint is too large".into(),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// A type that can be encoded into Kafka's binary wire format.
pub trait KafkaSerialize {
    /// Encode `self` into `buf`.
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError>;
}

/// A type that can be decoded from Kafka's binary wire format.
pub trait KafkaDeserialize: Sized {
    /// Decode an instance of `Self` from `buf`.
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError>;
}

// ---------------------------------------------------------------------------
// Implementations for primitive types
// ---------------------------------------------------------------------------

impl KafkaSerialize for i8 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_i8(*self);
        Ok(())
    }
}

impl KafkaDeserialize for i8 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 1 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_i8())
    }
}

impl KafkaSerialize for i16 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_i16(*self);
        Ok(())
    }
}

impl KafkaDeserialize for i16 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 2 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_i16())
    }
}

impl KafkaSerialize for i32 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_i32(*self);
        Ok(())
    }
}

impl KafkaDeserialize for i32 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_i32())
    }
}

impl KafkaSerialize for i64 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_i64(*self);
        Ok(())
    }
}

impl KafkaDeserialize for i64 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 8 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_i64())
    }
}

impl KafkaSerialize for f64 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_f64(*self);
        Ok(())
    }
}

impl KafkaDeserialize for f64 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 8 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_f64())
    }
}

impl KafkaSerialize for u32 {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_u32(*self);
        Ok(())
    }
}

impl KafkaDeserialize for u32 {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_u32())
    }
}

// ---------------------------------------------------------------------------
// bool → int8 (0 / 1)
// ---------------------------------------------------------------------------

impl KafkaSerialize for bool {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(())
    }
}

impl KafkaDeserialize for bool {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 1 {
            return Err(DecodeError::InsufficientBytes);
        }
        Ok(buf.get_u8() != 0)
    }
}

// ---------------------------------------------------------------------------
// varint (zig-zag encoded i32)
// ---------------------------------------------------------------------------

impl KafkaSerialize for super::types::VarInt {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        // zig-zag: (n << 1) ^ (n >> 31)
        let unsigned = ((self.0 << 1) ^ (self.0 >> 31)) as u64;
        encode_unsigned_varint(unsigned, buf);
        Ok(())
    }
}

impl KafkaDeserialize for super::types::VarInt {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let (unsigned, _) = decode_unsigned_varint(buf)?;
        // zig-zag decode: (n >> 1) ^ -(n & 1)
        let value = ((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32));
        Ok(super::types::VarInt(value))
    }
}

// ---------------------------------------------------------------------------
// varlong (zig-zag encoded i64)
// ---------------------------------------------------------------------------

impl KafkaSerialize for super::types::VarLong {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        let unsigned = ((self.0 << 1) ^ (self.0 >> 63)) as u64;
        encode_unsigned_varint(unsigned, buf);
        Ok(())
    }
}

impl KafkaDeserialize for super::types::VarLong {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let (unsigned, _) = decode_unsigned_varint(buf)?;
        let value = ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64));
        Ok(super::types::VarLong(value))
    }
}

// ---------------------------------------------------------------------------
// String / NullableString
// ---------------------------------------------------------------------------

impl KafkaSerialize for String {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        let len = self.len();
        if len > i16::MAX as usize {
            return Err(EncodeError::ValueTooLarge {
                message: format!("string length {len} exceeds i16::MAX"),
            });
        }
        buf.put_i16(len as i16);
        buf.put_slice(self.as_bytes());
        Ok(())
    }
}

impl KafkaDeserialize for String {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 2 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i16();
        match len {
            -1 => Err(DecodeError::UnexpectedNull),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative string length {n}"),
            }),
            n => {
                let n = n as usize;
                if buf.remaining() < n {
                    return Err(DecodeError::InsufficientBytes);
                }
                let bytes = &buf.copy_to_bytes(n)[..];
                Ok(std::str::from_utf8(bytes)?.to_owned())
            }
        }
    }
}

/// A nullable string encoded as a 2-byte length + UTF-8 bytes, with -1 for null.
impl KafkaSerialize for Option<String> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        match self {
            None => {
                buf.put_i16(-1);
                Ok(())
            }
            Some(s) => s.encode(buf),
        }
    }
}

impl KafkaDeserialize for Option<String> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 2 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i16();
        match len {
            -1 => Ok(None),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative nullable-string length {n}"),
            }),
            n => {
                let n = n as usize;
                if buf.remaining() < n {
                    return Err(DecodeError::InsufficientBytes);
                }
                let bytes = &buf.copy_to_bytes(n)[..];
                Ok(Some(std::str::from_utf8(bytes)?.to_owned()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Bytes / NullableBytes
// ---------------------------------------------------------------------------

impl KafkaSerialize for Vec<u8> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        let len = self.len();
        if len > i32::MAX as usize {
            return Err(EncodeError::ValueTooLarge {
                message: format!("bytes length {len} exceeds i32::MAX"),
            });
        }
        buf.put_i32(len as i32);
        buf.put_slice(self);
        Ok(())
    }
}

impl KafkaDeserialize for Vec<u8> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i32();
        match len {
            -1 => Err(DecodeError::UnexpectedNull),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative bytes length {n}"),
            }),
            n => {
                let n = n as usize;
                if buf.remaining() < n {
                    return Err(DecodeError::InsufficientBytes);
                }
                Ok(buf.copy_to_bytes(n).to_vec())
            }
        }
    }
}

/// Nullable bytes: 4-byte length prefix + raw bytes, -1 ⇒ null.
impl KafkaSerialize for Option<Vec<u8>> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        match self {
            None => {
                buf.put_i32(-1);
                Ok(())
            }
            Some(b) => b.encode(buf),
        }
    }
}

impl KafkaDeserialize for Option<Vec<u8>> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i32();
        match len {
            -1 => Ok(None),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative nullable-bytes length {n}"),
            }),
            n => {
                let n = n as usize;
                if buf.remaining() < n {
                    return Err(DecodeError::InsufficientBytes);
                }
                Ok(Some(buf.copy_to_bytes(n).to_vec()))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Array (Vec<T>)
// ---------------------------------------------------------------------------

impl<T: KafkaSerialize> KafkaSerialize for Vec<T> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        let len = self.len();
        if len > i32::MAX as usize {
            return Err(EncodeError::ValueTooLarge {
                message: format!("array length {len} exceeds i32::MAX"),
            });
        }
        buf.put_i32(len as i32);
        for item in self {
            item.encode(buf)?;
        }
        Ok(())
    }
}

impl<T: KafkaDeserialize> KafkaDeserialize for Vec<T> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i32();
        match len {
            -1 => Err(DecodeError::UnexpectedNull),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative array length {n}"),
            }),
            n => {
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(T::decode(buf)?);
                }
                Ok(items)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Nullable array (Option<Vec<T>>)
// ---------------------------------------------------------------------------

impl<T: KafkaSerialize> KafkaSerialize for Option<Vec<T>> {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        match self {
            None => {
                buf.put_i32(-1);
                Ok(())
            }
            Some(v) => v.encode(buf),
        }
    }
}

impl<T: KafkaDeserialize> KafkaDeserialize for Option<Vec<T>> {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 4 {
            return Err(DecodeError::InsufficientBytes);
        }
        let len = buf.get_i32();
        match len {
            -1 => Ok(None),
            n if n < 0 => Err(DecodeError::InvalidLength {
                message: format!("negative nullable-array length {n}"),
            }),
            n => {
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(T::decode(buf)?);
                }
                Ok(Some(items))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UUID (16 raw bytes)
// ---------------------------------------------------------------------------

impl KafkaSerialize for [u8; 16] {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        buf.put_slice(&self[..]);
        Ok(())
    }
}

impl KafkaDeserialize for [u8; 16] {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        if buf.remaining() < 16 {
            return Err(DecodeError::InsufficientBytes);
        }
        let mut out = [0u8; 16];
        buf.copy_to_slice(&mut out);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_roundtrip_i8() {
        let mut buf = BytesMut::new();
        42i8.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 1);
        let mut read: &[u8] = &buf;
        let val = i8::decode(&mut read).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn test_roundtrip_i16() {
        let mut buf = BytesMut::new();
        0x0102i16.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 2);
        let mut read: &[u8] = &buf;
        let val = i16::decode(&mut read).unwrap();
        assert_eq!(val, 0x0102);
    }

    #[test]
    fn test_roundtrip_i32() {
        let mut buf = BytesMut::new();
        0x01020304i32.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        let mut read: &[u8] = &buf;
        let val = i32::decode(&mut read).unwrap();
        assert_eq!(val, 0x01020304);
    }

    #[test]
    fn test_roundtrip_i64() {
        let mut buf = BytesMut::new();
        0x0102030405060708i64.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 8);
        let mut read: &[u8] = &buf;
        let val = i64::decode(&mut read).unwrap();
        assert_eq!(val, 0x0102030405060708);
    }

    #[test]
    fn test_roundtrip_bool() {
        let mut buf = BytesMut::new();
        true.encode(&mut buf).unwrap();
        false.encode(&mut buf).unwrap();
        let mut read: &[u8] = &buf;
        assert!(bool::decode(&mut read).unwrap());
        assert!(!bool::decode(&mut read).unwrap());
    }

    #[test]
    fn test_roundtrip_string() {
        let mut buf = BytesMut::new();
        "hello".to_owned().encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 7); // 2 length + 5 bytes
        let mut read: &[u8] = &buf;
        let val = String::decode(&mut read).unwrap();
        assert_eq!(val, "hello");
    }

    #[test]
    fn test_nullable_string_roundtrip() {
        let mut buf = BytesMut::new();
        let some: Option<String> = Some("foo".into());
        some.encode(&mut buf).unwrap();
        let none: Option<String> = None;
        none.encode(&mut buf).unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<String>::decode(&mut read).unwrap(),
            Some("foo".into())
        );
        assert_eq!(Option::<String>::decode(&mut read).unwrap(), None);
    }

    #[test]
    fn test_nullable_string_decode_error_on_nonnull_string() {
        // String decode should reject -1 length
        let mut buf = BytesMut::new();
        buf.put_i16(-1);
        let mut read: &[u8] = &buf;
        assert!(matches!(
            String::decode(&mut read),
            Err(DecodeError::UnexpectedNull)
        ));
    }

    #[test]
    fn test_roundtrip_bytes() {
        let data = vec![0x00u8, 0x01, 0x02, 0x03];
        let mut buf = BytesMut::new();
        data.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 8); // 4 length + 4 bytes
        let mut read: &[u8] = &buf;
        let val = Vec::<u8>::decode(&mut read).unwrap();
        assert_eq!(val, data);
    }

    #[test]
    fn test_roundtrip_nullable_bytes() {
        let mut buf = BytesMut::new();
        let some: Option<Vec<u8>> = Some(vec![1, 2, 3]);
        some.encode(&mut buf).unwrap();
        let none: Option<Vec<u8>> = None;
        none.encode(&mut buf).unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<Vec<u8>>::decode(&mut read).unwrap(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(Option::<Vec<u8>>::decode(&mut read).unwrap(), None);
    }

    #[test]
    fn test_roundtrip_array() {
        let items = vec![1i32, 2, 3, 4];
        let mut buf = BytesMut::new();
        items.encode(&mut buf).unwrap();
        // 4 length + 4*4 bytes
        assert_eq!(buf.len(), 20);
        let mut read: &[u8] = &buf;
        let val = Vec::<i32>::decode(&mut read).unwrap();
        assert_eq!(val, items);
    }

    #[test]
    fn test_roundtrip_nullable_array() {
        let mut buf = BytesMut::new();
        let some: Option<Vec<i16>> = Some(vec![10, 20]);
        some.encode(&mut buf).unwrap();
        let none: Option<Vec<i16>> = None;
        none.encode(&mut buf).unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<Vec<i16>>::decode(&mut read).unwrap(),
            Some(vec![10, 20])
        );
        assert_eq!(Option::<Vec<i16>>::decode(&mut read).unwrap(), None);
    }

    #[test]
    fn test_roundtrip_uuid() {
        let uuid = [
            0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let mut buf = BytesMut::new();
        uuid.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 16);
        let mut read: &[u8] = &buf;
        let val = <[u8; 16]>::decode(&mut read).unwrap();
        assert_eq!(val, uuid);
    }

    #[test]
    fn test_roundtrip_varint() {
        use super::super::types::VarInt;
        for val in [0i32, 1, -1, 127, -128, 16383, -16384, 2000000, -2000000] {
            let mut buf = BytesMut::new();
            VarInt(val).encode(&mut buf).unwrap();
            let mut read: &[u8] = &buf;
            let decoded = VarInt::decode(&mut read).unwrap();
            assert_eq!(decoded.0, val, "varint roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_roundtrip_varlong() {
        use super::super::types::VarLong;
        for val in [0i64, 1, -1, 1 << 40, -(1 << 40)] {
            let mut buf = BytesMut::new();
            VarLong(val).encode(&mut buf).unwrap();
            let mut read: &[u8] = &buf;
            let decoded = VarLong::decode(&mut read).unwrap();
            assert_eq!(decoded.0, val, "varlong roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_insufficient_bytes_i32() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        let mut read: &[u8] = &buf;
        assert!(matches!(
            i32::decode(&mut read),
            Err(DecodeError::InsufficientBytes)
        ));
    }

    #[test]
    fn test_empty_array() {
        let items: Vec<u8> = vec![];
        let mut buf = BytesMut::new();
        items.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        let mut read: &[u8] = &buf;
        let val = Vec::<u8>::decode(&mut read).unwrap();
        assert!(val.is_empty());
    }
}
