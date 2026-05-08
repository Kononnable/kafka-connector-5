#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
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
    /// True if the leader must skip running the assignment.
    /// Available in version 9+.
    pub skip_assignment: bool,
    /// The member ID assigned by the group coordinator.
    pub member_id: String,
    /// The group members.
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
        crate::traits::ApiVersion::new(9)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (9),
            "version {} is not supported by {} (supported: 0-9)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (2) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.error_code.encode(buf, version, is_flexible)?;
        self.generation_id.encode(buf, version, is_flexible)?;
        if (7) <= version.0 {
            self.protocol_type.encode(buf, version, is_flexible)?;
        } else if self.protocol_type.is_some() {
            return Err(SerializationError::Encode(
                "field 'ProtocolType' is not available in this version",
            ));
        }
        self.protocol_name.encode(buf, version, is_flexible)?;
        self.leader.encode(buf, version, is_flexible)?;
        if (9) <= version.0 {
            self.skip_assignment.encode(buf, version, is_flexible)?;
        } else if self.skip_assignment {
            return Err(SerializationError::Encode(
                "field 'SkipAssignment' is not available in this version",
            ));
        }
        self.member_id.encode(buf, version, is_flexible)?;
        self.members.encode(buf, version, is_flexible)?;
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
        let throttle_time_ms = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let generation_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let protocol_type = if (7) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_name =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let skip_assignment = if (9) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let members =
            <Vec<JoinGroupResponseMember> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment,
            member_id,
            members,
        })
    }
}
impl KafkaSerialize for JoinGroupResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (2) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.error_code.encode(buf, version, is_flexible)?;
        self.generation_id.encode(buf, version, is_flexible)?;
        if (7) <= version.0 {
            self.protocol_type.encode(buf, version, is_flexible)?;
        }
        self.protocol_name.encode(buf, version, is_flexible)?;
        self.leader.encode(buf, version, is_flexible)?;
        if (9) <= version.0 {
            self.skip_assignment.encode(buf, version, is_flexible)?;
        }
        self.member_id.encode(buf, version, is_flexible)?;
        self.members.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = if (2) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let generation_id = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let protocol_type = if (7) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_name =
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let leader = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let skip_assignment = if (9) <= version.0 {
            <bool as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let members =
            <Vec<JoinGroupResponseMember> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
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
            skip_assignment,
            member_id,
            members,
        })
    }
}

impl KafkaSerialize for JoinGroupResponseMember {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.member_id.encode(buf, version, is_flexible)?;
        if (5) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        self.metadata.encode(buf, version, is_flexible)?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupResponseMember {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let member_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_instance_id = if (5) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let metadata = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
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
