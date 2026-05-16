#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// KRaftVersionRecord
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KRaftVersionRecord {
    /// The version of the kraft version record.
    pub version: i16,
    /// The kraft protocol version.
    pub kraft_version: i16,
}

impl KafkaCodec for KRaftVersionRecord {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.version.encode(buf, version, is_flexible)?;
        self.kraft_version.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let version_val = KafkaCodec::decode(buf, version, is_flexible)?;
        let kraft_version = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            version: version_val,
            kraft_version,
        })
    }
}
