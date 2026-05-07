//! Kafka wire protocol types.
//!
//! Newtype wrappers for protcol-level types that don't have a direct
//! Rust primitive (e.g. varint, varlong).

use std::fmt;

// ---------------------------------------------------------------------------
// VarInt
// ---------------------------------------------------------------------------

/// A variable-length integer encoded with zig-zag.
///
/// On the wire this is an unsigned varint whose value is `(n << 1) ^ (n >> 31)`,
/// so small negative/positive values occupy 1–3 bytes instead of always 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VarInt(pub i32);

impl VarInt {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }
}

impl fmt::Display for VarInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i32> for VarInt {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<VarInt> for i32 {
    fn from(v: VarInt) -> Self {
        v.0
    }
}

// ---------------------------------------------------------------------------
// VarLong
// ---------------------------------------------------------------------------

/// A variable-length long integer encoded with zig-zag.
///
/// On the wire this is an unsigned varint whose value is `(n << 1) ^ (n >> 63)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VarLong(pub i64);

impl VarLong {
    pub const fn new(value: i64) -> Self {
        Self(value)
    }
}

impl fmt::Display for VarLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for VarLong {
    fn from(v: i64) -> Self {
        Self(v)
    }
}

impl From<VarLong> for i64 {
    fn from(v: VarLong) -> Self {
        v.0
    }
}

// ---------------------------------------------------------------------------
// CompactArray length encoding helper
// ---------------------------------------------------------------------------

/// Compute the unsigned varint-encoded length for a compact array/string/bytes
/// whose element count (or byte length) is `n`.  Compact arrays use
/// `unsigned_varint(n + 1)`, where 0 means null.
pub fn compact_length(n: usize) -> u64 {
    (n as u64) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_from_into() {
        let v: VarInt = 42.into();
        assert_eq!(v.0, 42);
        let n: i32 = v.into();
        assert_eq!(n, 42);
    }

    #[test]
    fn test_varlong_from_into() {
        let v: VarLong = 1_000_000_000_000i64.into();
        assert_eq!(v.0, 1_000_000_000_000);
        let n: i64 = v.into();
        assert_eq!(n, 1_000_000_000_000);
    }

    #[test]
    fn test_compact_length() {
        assert_eq!(compact_length(0), 1);
        assert_eq!(compact_length(1), 2);
        assert_eq!(compact_length(127), 128);
    }
}
