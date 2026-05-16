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
use indexmap::IndexMap;

pub use crate::traits::SerializationError;

// ---------------------------------------------------------------------------
// Helper: unsigned varint encoding (used internally by varint / varlong)
// ---------------------------------------------------------------------------

/// Encode `value` as an unsigned variable-length integer.
/// Returns the number of bytes written.
pub fn encode_unsigned_varint<B: BufMut>(mut value: u64, buf: &mut B) -> usize {
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
pub fn decode_unsigned_varint<B: Buf>(buf: &mut B) -> Result<(u64, usize), SerializationError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    let mut consumed: usize = 0;
    loop {
        if !buf.has_remaining() {
            panic!("insufficient bytes");
        }
        let byte = buf.get_u8();
        consumed += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }
        shift += 7;
        if shift >= 64 {
            panic!("varint value exceeds 64-bit maximum — malformed data");
        }
    }
}

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// A type that can be encoded/decoded to/from Kafka's binary wire format.
pub trait KafkaCodec: Sized {
    /// Encode `self` into `buf` for the given API `version`.
    ///
    /// `is_flexible` selects compact (unsigned varint) vs classic encoding
    /// for variable-length types (string, bytes, array).  Fixed-width types
    /// (i8, i16, i32, etc.) ignore this parameter.
    ///
    /// The generated struct impls use `version` for per-field version gating
    /// and `is_flexible` for compact wire format selection.
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError>;

    /// Decode an instance of `Self` from `buf` for the given API `version`.
    ///
    /// `version` is used for version-gated field presence in struct impls.
    /// `is_flexible` selects compact vs classic encoding for variable-length types.
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError>;
}

// ---------------------------------------------------------------------------
// Implementations for primitive types
// ---------------------------------------------------------------------------

impl KafkaCodec for i8 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_i8(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 1 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_i8())
    }
}

impl KafkaCodec for i16 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_i16(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 2 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_i16())
    }
}

impl KafkaCodec for i32 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_i32(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 4 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_i32())
    }
}

impl KafkaCodec for i64 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_i64(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 8 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_i64())
    }
}

impl KafkaCodec for f64 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_f64(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 8 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_f64())
    }
}

impl KafkaCodec for u32 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_u32(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 4 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_u32())
    }
}

impl KafkaCodec for u16 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_u16(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 2 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_u16())
    }
}

// ---------------------------------------------------------------------------
// u8 (single byte, used by Vec<u8> via blanket Vec<T> impl)
// ---------------------------------------------------------------------------

impl KafkaCodec for u8 {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_u8(*self);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 1 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_u8())
    }
}

// ---------------------------------------------------------------------------
// bool → int8 (0 / 1)
// ---------------------------------------------------------------------------

impl KafkaCodec for bool {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_u8(if *self { 1 } else { 0 });
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 1 {
            panic!("insufficient bytes");
        }
        Ok(buf.get_u8() != 0)
    }
}

// ---------------------------------------------------------------------------
// varint (zig-zag encoded i32)
// ---------------------------------------------------------------------------

impl KafkaCodec for super::types::VarInt {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        // zig-zag: (n << 1) ^ (n >> 31)
        let unsigned = ((self.0 << 1) ^ (self.0 >> 31)) as u64;
        encode_unsigned_varint(unsigned, buf);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let (unsigned, _) = decode_unsigned_varint(buf)?;
        // zig-zag decode: (n >> 1) ^ -(n & 1)
        let value = ((unsigned >> 1) as i32) ^ (-((unsigned & 1) as i32));
        Ok(super::types::VarInt(value))
    }
}

// ---------------------------------------------------------------------------
// varlong (zig-zag encoded i64)
// ---------------------------------------------------------------------------

impl KafkaCodec for super::types::VarLong {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        let unsigned = ((self.0 << 1) ^ (self.0 >> 63)) as u64;
        encode_unsigned_varint(unsigned, buf);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let (unsigned, _) = decode_unsigned_varint(buf)?;
        let value = ((unsigned >> 1) as i64) ^ (-((unsigned & 1) as i64));
        Ok(super::types::VarLong(value))
    }
}

// ---------------------------------------------------------------------------
// String / NullableString
// ---------------------------------------------------------------------------

impl KafkaCodec for String {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            // Compact string: unsigned varint(length + 1) + UTF-8
            let len = self.len() as u64 + 1;
            encode_unsigned_varint(len, buf);
            buf.put_slice(self.as_bytes());
            Ok(())
        } else {
            // Classic string: 2-byte length + UTF-8
            let len = self.len();
            assert!(
                len <= i16::MAX as usize,
                "string length {len} exceeds i16::MAX"
            );
            buf.put_i16(len as i16);
            buf.put_slice(self.as_bytes());
            Ok(())
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            // Compact string: unsigned varint, value 0 = null, otherwise length = value - 1
            let (raw_len, _) = decode_unsigned_varint(buf)?;
            if raw_len == 0 {
                panic!("unexpected null");
            }
            let n = (raw_len - 1) as usize;
            if buf.remaining() < n {
                panic!("insufficient bytes");
            }
            let bytes = &buf.copy_to_bytes(n)[..];
            Ok(std::str::from_utf8(bytes)
                .expect("broker sent invalid UTF-8")
                .to_owned())
        } else {
            if buf.remaining() < 2 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i16();
            match len {
                -1 => panic!("unexpected null"),
                n if n < 0 => panic!("negative string length {n}"),
                n => {
                    let n = n as usize;
                    if buf.remaining() < n {
                        panic!("insufficient bytes");
                    }
                    let bytes = &buf.copy_to_bytes(n)[..];
                    Ok(std::str::from_utf8(bytes)
                        .expect("broker sent invalid UTF-8")
                        .to_owned())
                }
            }
        }
    }
}

/// A nullable string encoded as a 2-byte length + UTF-8 bytes, with -1 for null.
impl KafkaCodec for Option<String> {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            match self {
                None => {
                    encode_unsigned_varint(0, buf);
                    Ok(())
                }
                Some(s) => {
                    encode_unsigned_varint(s.len() as u64 + 1, buf);
                    buf.put_slice(s.as_bytes());
                    Ok(())
                }
            }
        } else {
            match self {
                None => {
                    buf.put_i16(-1);
                    Ok(())
                }
                Some(s) => {
                    let len = s.len();
                    assert!(
                        len <= i16::MAX as usize,
                        "string length {len} exceeds i16::MAX"
                    );
                    buf.put_i16(len as i16);
                    buf.put_slice(s.as_bytes());
                    Ok(())
                }
            }
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (raw_len, _) = decode_unsigned_varint(buf)?;
            if raw_len == 0 {
                return Ok(None);
            }
            let n = (raw_len - 1) as usize;
            if buf.remaining() < n {
                panic!("insufficient bytes");
            }
            let bytes = &buf.copy_to_bytes(n)[..];
            Ok(Some(
                std::str::from_utf8(bytes)
                    .expect("broker sent invalid UTF-8")
                    .to_owned(),
            ))
        } else {
            if buf.remaining() < 2 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i16();
            match len {
                -1 => Ok(None),
                n if n < 0 => panic!("negative nullable-string length {n}"),
                n => {
                    let n = n as usize;
                    if buf.remaining() < n {
                        panic!("insufficient bytes");
                    }
                    let bytes = &buf.copy_to_bytes(n)[..];
                    Ok(Some(
                        std::str::from_utf8(bytes)
                            .expect("broker sent invalid UTF-8")
                            .to_owned(),
                    ))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Array (Vec<T>)
// ---------------------------------------------------------------------------

impl<T: KafkaCodec> KafkaCodec for Vec<T> {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            encode_unsigned_varint(self.len() as u64 + 1, buf);
            for item in self {
                item.encode(buf, _version, true)?;
            }
            Ok(())
        } else {
            let len = self.len();
            assert!(
                len <= i32::MAX as usize,
                "array length {len} exceeds i32::MAX"
            );
            buf.put_i32(len as i32);
            for item in self {
                item.encode(buf, _version, false)?;
            }
            Ok(())
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (raw_count, _) = decode_unsigned_varint(buf)?;
            if raw_count == 0 {
                panic!("unexpected null");
            }
            let n = (raw_count - 1) as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(T::decode(buf, _version, true)?);
            }
            Ok(items)
        } else {
            if buf.remaining() < 4 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i32();
            match len {
                -1 => panic!("unexpected null"),
                n if n < 0 => panic!("negative array length {n}"),
                n => {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(T::decode(buf, _version, false)?);
                    }
                    Ok(items)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Nullable array (Option<Vec<T>>)
// ---------------------------------------------------------------------------

impl<T: KafkaCodec> KafkaCodec for Option<Vec<T>> {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            match self {
                None => {
                    encode_unsigned_varint(0, buf);
                    Ok(())
                }
                Some(v) => {
                    encode_unsigned_varint(v.len() as u64 + 1, buf);
                    for item in v {
                        item.encode(buf, _version, true)?;
                    }
                    Ok(())
                }
            }
        } else {
            match self {
                None => {
                    buf.put_i32(-1);
                    Ok(())
                }
                Some(v) => v.encode(buf, _version, false),
            }
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (raw_count, _) = decode_unsigned_varint(buf)?;
            if raw_count == 0 {
                return Ok(None);
            }
            let n = (raw_count - 1) as usize;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(T::decode(buf, _version, true)?);
            }
            Ok(Some(items))
        } else {
            if buf.remaining() < 4 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i32();
            match len {
                -1 => Ok(None),
                n if n < 0 => panic!("negative nullable-array length {n}"),
                n => {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(T::decode(buf, _version, false)?);
                    }
                    Ok(Some(items))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IndexMap<K, V> — serialized identically to Vec<(K, V)>
// ---------------------------------------------------------------------------

impl<K: KafkaCodec + std::hash::Hash + Eq, V: KafkaCodec> KafkaCodec for IndexMap<K, V> {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            encode_unsigned_varint(self.len() as u64 + 1, buf);
            for (key, val) in self.iter() {
                key.encode(buf, version, true)?;
                val.encode(buf, version, true)?;
            }
            Ok(())
        } else {
            let len = self.len();
            assert!(
                len <= i32::MAX as usize,
                "IndexMap length {len} exceeds i32::MAX"
            );
            buf.put_i32(len as i32);
            for (key, val) in self.iter() {
                key.encode(buf, version, false)?;
                val.encode(buf, version, false)?;
            }
            Ok(())
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (raw_count, _) = decode_unsigned_varint(buf)?;
            if raw_count == 0 {
                panic!("unexpected null");
            }
            let n = (raw_count - 1) as usize;
            let mut map = IndexMap::with_capacity(n);
            for _ in 0..n {
                let key = K::decode(buf, version, true)?;
                let val = V::decode(buf, version, true)?;
                map.insert(key, val);
            }
            Ok(map)
        } else {
            if buf.remaining() < 4 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i32();
            match len {
                -1 => panic!("unexpected null"),
                n if n < 0 => panic!("negative IndexMap length {n}"),
                n => {
                    let mut map = IndexMap::with_capacity(n as usize);
                    for _ in 0..n {
                        let key = K::decode(buf, version, false)?;
                        let val = V::decode(buf, version, false)?;
                        map.insert(key, val);
                    }
                    Ok(map)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Option<IndexMap<K, V>> — nullable IndexMap
// ---------------------------------------------------------------------------

impl<K: KafkaCodec + std::hash::Hash + Eq, V: KafkaCodec> KafkaCodec for Option<IndexMap<K, V>> {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if is_flexible {
            match self {
                None => {
                    encode_unsigned_varint(0, buf);
                    Ok(())
                }
                Some(map) => {
                    encode_unsigned_varint(map.len() as u64 + 1, buf);
                    for (key, val) in map.iter() {
                        key.encode(buf, version, true)?;
                        val.encode(buf, version, true)?;
                    }
                    Ok(())
                }
            }
        } else {
            match self {
                None => {
                    buf.put_i32(-1);
                    Ok(())
                }
                Some(map) => {
                    assert!(
                        map.len() <= i32::MAX as usize,
                        "nullable IndexMap length {} exceeds i32::MAX",
                        map.len()
                    );
                    buf.put_i32(map.len() as i32);
                    for (key, val) in map.iter() {
                        key.encode(buf, version, false)?;
                        val.encode(buf, version, false)?;
                    }
                    Ok(())
                }
            }
        }
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if is_flexible {
            let (raw_count, _) = decode_unsigned_varint(buf)?;
            if raw_count == 0 {
                return Ok(None);
            }
            let n = (raw_count - 1) as usize;
            let mut map = IndexMap::with_capacity(n);
            for _ in 0..n {
                let key = K::decode(buf, version, true)?;
                let val = V::decode(buf, version, true)?;
                map.insert(key, val);
            }
            Ok(Some(map))
        } else {
            if buf.remaining() < 4 {
                panic!("insufficient bytes");
            }
            let len = buf.get_i32();
            match len {
                -1 => Ok(None),
                n if n < 0 => panic!("negative nullable IndexMap length {n}"),
                n => {
                    let mut map = IndexMap::with_capacity(n as usize);
                    for _ in 0..n {
                        let key = K::decode(buf, version, false)?;
                        let val = V::decode(buf, version, false)?;
                        map.insert(key, val);
                    }
                    Ok(Some(map))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// UUID (16 raw bytes)
// ---------------------------------------------------------------------------

impl KafkaCodec for [u8; 16] {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<(), SerializationError> {
        buf.put_slice(&self[..]);
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        _version: crate::traits::ApiVersion,
        _is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        if buf.remaining() < 16 {
            panic!("insufficient bytes");
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
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_roundtrip_i8() {
        let mut buf = BytesMut::new();
        42i8.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 1);
        let mut read: &[u8] = &buf;
        let val = i8::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, 42);
    }

    #[test]
    fn test_roundtrip_i16() {
        let mut buf = BytesMut::new();
        0x0102i16
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 2);
        let mut read: &[u8] = &buf;
        let val = i16::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, 0x0102);
    }

    #[test]
    fn test_roundtrip_i32() {
        let mut buf = BytesMut::new();
        0x01020304i32
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 4);
        let mut read: &[u8] = &buf;
        let val = i32::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, 0x01020304);
    }

    #[test]
    fn test_roundtrip_i64() {
        let mut buf = BytesMut::new();
        0x0102030405060708i64
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 8);
        let mut read: &[u8] = &buf;
        let val = i64::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, 0x0102030405060708);
    }

    #[test]
    fn test_roundtrip_bool() {
        let mut buf = BytesMut::new();
        true.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        false
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let mut read: &[u8] = &buf;
        assert!(bool::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap());
        assert!(!bool::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap());
    }

    #[test]
    fn test_roundtrip_string() {
        let mut buf = BytesMut::new();
        "hello"
            .to_owned()
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 7); // 2 length + 5 bytes
        let mut read: &[u8] = &buf;
        let val = String::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, "hello");
    }

    #[test]
    fn test_nullable_string_roundtrip() {
        let mut buf = BytesMut::new();
        let some: Option<String> = Some("foo".into());
        some.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let none: Option<String> = None;
        none.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<String>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap(),
            Some("foo".into())
        );
        assert_eq!(
            Option::<String>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap(),
            None
        );
    }

    #[test]
    #[should_panic(expected = "unexpected null")]
    fn test_nullable_string_decode_error_on_nonnull_string() {
        // String decode should reject -1 length
        let mut buf = BytesMut::new();
        buf.put_i16(-1);
        let mut read: &[u8] = &buf;
        String::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
    }

    #[test]
    fn test_roundtrip_bytes() {
        let data = vec![0x00u8, 0x01, 0x02, 0x03];
        let mut buf = BytesMut::new();
        data.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 8); // 4 length + 4 bytes
        let mut read: &[u8] = &buf;
        let val = Vec::<u8>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, data);
    }

    #[test]
    fn test_roundtrip_nullable_bytes() {
        let mut buf = BytesMut::new();
        let some: Option<Vec<u8>> = Some(vec![1, 2, 3]);
        some.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let none: Option<Vec<u8>> = None;
        none.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<Vec<u8>>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            Option::<Vec<u8>>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap(),
            None
        );
    }

    #[test]
    fn test_roundtrip_array() {
        let items = vec![1i32, 2, 3, 4];
        let mut buf = BytesMut::new();
        items
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        // 4 length + 4*4 bytes
        assert_eq!(buf.len(), 20);
        let mut read: &[u8] = &buf;
        let val = Vec::<i32>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, items);
    }

    #[test]
    fn test_roundtrip_nullable_array() {
        let mut buf = BytesMut::new();
        let some: Option<Vec<i16>> = Some(vec![10, 20]);
        some.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let none: Option<Vec<i16>> = None;
        none.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        let mut read: &[u8] = &buf;
        assert_eq!(
            Option::<Vec<i16>>::decode(&mut read, crate::traits::ApiVersion::new(0), false)
                .unwrap(),
            Some(vec![10, 20])
        );
        assert_eq!(
            Option::<Vec<i16>>::decode(&mut read, crate::traits::ApiVersion::new(0), false)
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_roundtrip_uuid() {
        let uuid = [
            0x00u8, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let mut buf = BytesMut::new();
        uuid.encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 16);
        let mut read: &[u8] = &buf;
        let val = <[u8; 16]>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert_eq!(val, uuid);
    }

    #[test]
    fn test_roundtrip_varint() {
        use super::super::types::VarInt;
        for val in [0i32, 1, -1, 127, -128, 16383, -16384, 2000000, -2000000] {
            let mut buf = BytesMut::new();
            VarInt(val)
                .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
                .unwrap();
            let mut read: &[u8] = &buf;
            let decoded =
                VarInt::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
            assert_eq!(decoded.0, val, "varint roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_roundtrip_varlong() {
        use super::super::types::VarLong;
        for val in [0i64, 1, -1, 1 << 40, -(1 << 40)] {
            let mut buf = BytesMut::new();
            VarLong(val)
                .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
                .unwrap();
            let mut read: &[u8] = &buf;
            let decoded =
                VarLong::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
            assert_eq!(decoded.0, val, "varlong roundtrip failed for {val}");
        }
    }

    #[test]
    #[should_panic(expected = "insufficient bytes")]
    fn test_insufficient_bytes_i32() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        let mut read: &[u8] = &buf;
        i32::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
    }

    #[test]
    fn test_empty_array() {
        let items: Vec<u8> = vec![];
        let mut buf = BytesMut::new();
        items
            .encode(&mut buf, crate::traits::ApiVersion::new(0), false)
            .unwrap();
        assert_eq!(buf.len(), 4);
        let mut read: &[u8] = &buf;
        let val = Vec::<u8>::decode(&mut read, crate::traits::ApiVersion::new(0), false).unwrap();
        assert!(val.is_empty());
    }
}
