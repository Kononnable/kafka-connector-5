#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeDelegationTokenRequest {
    /// Each owner that we want to describe delegation tokens for, or null to describe all tokens.
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeDelegationTokenOwner {
    /// The owner principal type.
    pub principal_type: String,
    /// The owner principal name.
    pub principal_name: String,
}

impl ApiRequest for DescribeDelegationTokenRequest {
    type Response = crate::generated::DescribeDelegationTokenResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(41)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(1)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 0-1)",
            version.0,
            stringify!(Self)
        );
        self.owners
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Owners"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let owners = <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Owners"))?;
        Ok(Self { owners })
    }
}
impl KafkaSerialize for DescribeDelegationTokenRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.owners
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Owners".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let owners = <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Owners".into(),
        })?;
        Ok(Self { owners })
    }
}

impl KafkaSerialize for DescribeDelegationTokenOwner {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.principal_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenOwner {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        let principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
}
