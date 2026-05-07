#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// JoinGroupResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 2+.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The generation ID of the group.
    pub generation_id: i32,
    /// The group protocol name.
    /// Available in version 7+.
    pub protocol_type: Option<String>,
    /// The group protocol selected by the coordinator.
    pub protocol_name: Option<String>,
    /// The leader of the group.
    pub leader: String,
    /// The member ID assigned by the group coordinator.
    pub member_id: String,
    /// Members. Type: []JoinGroupResponseMember.
    pub members: Vec<JoinGroupResponseMember>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinGroupResponseMember {
    /// The group member ID.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 5+.
    pub group_instance_id: Option<String>,
    /// The group member metadata.
    pub metadata: Vec<u8>,
}

impl ApiResponse for JoinGroupResponse {
    type Request = crate::generated::JoinGroupRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(11)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(7)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (7),
            "version {} is not supported by {} (supported: 0-7)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (6) <= version.0;
        if (2) <= version.0 {
            self.throttle_time_ms
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ThrottleTimeMs"))?;
        }
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ErrorCode"))?;
        self.generation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode GenerationId"))?;
        if (7) <= version.0 {
            self.protocol_type
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode ProtocolType"))?;
        }
        self.protocol_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode ProtocolName"))?;
        self.leader
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Leader"))?;
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode MemberId"))?;
        self.members
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Members"))?;
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
        let is_flexible = (6) <= version.0;
        let throttle_time_ms = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ThrottleTimeMs"))?
        } else {
            Default::default()
        };
        let error_code = <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ErrorCode"))?;
        let generation_id = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode GenerationId"))?;
        let protocol_type = if (7) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode ProtocolType"))?
        } else {
            Default::default()
        };
        let protocol_name = <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode ProtocolName"))?;
        let leader = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode Leader"))?;
        let member_id = <String as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Decode("failed to decode MemberId"))?;
        let members =
            <Vec<JoinGroupResponseMember> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode Members"))?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }
}
impl KafkaSerialize for JoinGroupResponse {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.generation_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GenerationId".into(),
            })?;
        self.protocol_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolType".into(),
            })?;
        self.protocol_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolName".into(),
            })?;
        self.leader
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Leader".into(),
            })?;
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.members
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Members".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.throttle_time_ms
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ThrottleTimeMs".into(),
            })?;
        self.error_code
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ErrorCode".into(),
            })?;
        self.generation_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GenerationId".into(),
            })?;
        self.protocol_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolType".into(),
            })?;
        self.protocol_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode ProtocolName".into(),
            })?;
        self.leader
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Leader".into(),
            })?;
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.members
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Members".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupResponse {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let throttle_time_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode ErrorCode".into(),
            })?;
        let generation_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode GenerationId".into(),
            })?;
        let protocol_type = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ProtocolType".into(),
            }
        })?;
        let protocol_name = <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
            DecodeError::Protocol {
                message: "failed to decode ProtocolName".into(),
            }
        })?;
        let leader =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Leader".into(),
            })?;
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        let members =
            <Vec<JoinGroupResponseMember> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Members".into(),
                }
            })?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let throttle_time_ms = <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ThrottleTimeMs".into(),
            })?;
        let error_code =
            <i16 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode ErrorCode".into(),
                }
            })?;
        let generation_id =
            <i32 as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GenerationId".into(),
                }
            })?;
        let protocol_type = <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProtocolType".into(),
            })?;
        let protocol_name = <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode ProtocolName".into(),
            })?;
        let leader =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Leader".into(),
                }
            })?;
        let member_id =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberId".into(),
                }
            })?;
        let members =
            <Vec<JoinGroupResponseMember> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Members".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }
}

impl KafkaSerialize for JoinGroupResponseMember {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.member_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.group_instance_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupInstanceId".into(),
            })?;
        self.metadata
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metadata".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.member_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MemberId".into(),
            })?;
        self.group_instance_id
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode GroupInstanceId".into(),
            })?;
        self.metadata
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Metadata".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupResponseMember {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let member_id =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MemberId".into(),
            })?;
        let group_instance_id =
            <Option<String> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                }
            })?;
        let metadata =
            <Vec<u8> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Metadata".into(),
            })?;
        Ok(Self {
            member_id,
            group_instance_id,
            metadata,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let member_id =
            <String as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode MemberId".into(),
                }
            })?;
        let group_instance_id =
            <Option<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode GroupInstanceId".into(),
                },
            )?;
        let metadata =
            <Vec<u8> as KafkaDeserialize>::decode_flexible(buf, is_flexible).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode Metadata".into(),
                }
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            group_instance_id,
            metadata,
        })
    }
}
