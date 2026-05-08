#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// EnvelopeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EnvelopeRequest {
    /// The embedded request header and data.
    pub request_data: Vec<u8>,
    /// Value of the initial client principal when the request is redirected by a broker.
    pub request_principal: Option<Vec<u8>>,
    /// The original client's address in bytes.
    pub client_host_address: Vec<u8>,
}

impl ApiRequest for EnvelopeRequest {
    type Response = crate::generated::EnvelopeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(58)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.request_data.encode(buf, version, is_flexible)?;
        self.request_principal.encode(buf, version, is_flexible)?;
        self.client_host_address.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let request_data = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let request_principal =
            <Option<Vec<u8>> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_host_address = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
        })
    }
}
impl KafkaSerialize for EnvelopeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.request_data.encode(buf, version, is_flexible)?;
        self.request_principal.encode(buf, version, is_flexible)?;
        self.client_host_address.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for EnvelopeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let request_data = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let request_principal =
            <Option<Vec<u8>> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_host_address = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
        })
    }
}
