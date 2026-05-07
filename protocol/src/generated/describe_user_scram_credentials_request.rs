#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeUserScramCredentialsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeUserScramCredentialsRequest {
    /// The users to describe, or null/empty to describe all users.
    pub users: Option<Vec<UserName>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UserName {
    /// The user name.
    pub name: String,
}

impl ApiRequest for DescribeUserScramCredentialsRequest {
    type Response = crate::generated::DescribeUserScramCredentialsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(50)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        self.users
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode Users"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let users = <Option<Vec<UserName>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode Users"))?;
        Ok(Self { users })
    }
}
impl KafkaSerialize for DescribeUserScramCredentialsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.users
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Users".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for DescribeUserScramCredentialsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let users = <Option<Vec<UserName>> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode Users".into(),
            }
        })?;
        Ok(Self { users })
    }
}

impl KafkaSerialize for UserName {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Name".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UserName {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Name".into(),
            })?;
        Ok(Self { name })
    }
}
