#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
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
        self.request_data
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode RequestData"))?;
        self.request_principal
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode RequestPrincipal"))?;
        self.client_host_address
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode ClientHostAddress"))?;
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let request_data = <Vec<u8> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode RequestData"))?;
        let request_principal = <Option<Vec<u8>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode RequestPrincipal"))?;
        let client_host_address = <Vec<u8> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode ClientHostAddress"))?;
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
        })
    }
}
impl KafkaSerialize for EnvelopeRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.request_data
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RequestData".into(),
            })?;
        self.request_principal
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode RequestPrincipal".into(),
            })?;
        self.client_host_address
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ClientHostAddress".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for EnvelopeRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let request_data =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode RequestData".into(),
            })?;
        let request_principal =
            <Option<Vec<u8>> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode RequestPrincipal".into(),
                }
            })?;
        let client_host_address =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ClientHostAddress".into(),
            })?;
        Ok(Self {
            request_data,
            request_principal,
            client_host_address,
        })
    }
}
