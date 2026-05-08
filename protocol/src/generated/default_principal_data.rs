#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DefaultPrincipalData
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DefaultPrincipalData {
    /// The principal type.
    pub r#type: String,
    /// The principal name.
    pub name: String,
    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    pub token_authenticated: bool,
}

impl KafkaSerialize for DefaultPrincipalData {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.r#type.encode(buf, version, is_flexible)?;
        self.name.encode(buf, version, is_flexible)?;
        self.token_authenticated.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DefaultPrincipalData {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let r#type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let token_authenticated = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            r#type,
            name,
            token_authenticated,
        })
    }
}
