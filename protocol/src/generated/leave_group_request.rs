#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// LeaveGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LeaveGroupRequest {
    /// The ID of the group to leave.
    pub group_id: String,
    /// The member ID to remove from the group.
    pub member_id: String,
}

impl ApiRequest for LeaveGroupRequest {
    type Response = crate::generated::LeaveGroupResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(13)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.group_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode GroupId"))?;
        self.member_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode MemberId"))?;
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let group_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode GroupId"))?;
        let member_id = <String as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode MemberId"))?;
        Ok(Self {
            group_id,
            member_id,
        })
    }
}
impl KafkaSerialize for LeaveGroupRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.group_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupId".into(),
            })?;
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for LeaveGroupRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let group_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GroupId".into(),
            })?;
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        Ok(Self {
            group_id,
            member_id,
        })
    }
}
