#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeGroupsResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// Each described group.
    pub groups: Vec<DescribedGroup>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    pub error_code: i16,
    /// The describe error message, or null if there was no error.
    /// Available in version 6+.
    pub error_message: Option<String>,
    /// The group ID string.
    pub group_id: String,
    /// The group state string, or the empty string.
    pub group_state: String,
    /// The group protocol type, or the empty string.
    pub protocol_type: String,
    /// The group protocol data, or the empty string.
    pub protocol_data: String,
    /// The group members.
    pub members: Vec<DescribedGroupMember>,
    /// 32-bit bitfield to represent authorized operations for this group.
    /// Available in version 3+.
    pub authorized_operations: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribedGroupMember {
    /// The member id.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 4+.
    pub group_instance_id: Option<String>,
    /// The client ID used in the member's latest join group request.
    pub client_id: String,
    /// The client host.
    pub client_host: String,
    /// The metadata corresponding to the current group protocol in use.
    pub member_metadata: Vec<u8>,
    /// The current assignment provided by the group leader.
    pub member_assignment: Vec<u8>,
}

impl ApiResponse for DescribeGroupsResponse {
    type Request = crate::generated::DescribeGroupsRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(15)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}
impl KafkaSerialize for DescribeGroupsResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        self.groups.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeGroupsResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let groups = <Vec<DescribedGroup> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl KafkaSerialize for DescribedGroup {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.error_code.encode(buf, version, is_flexible)?;
        if (6) <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        self.group_id.encode(buf, version, is_flexible)?;
        self.group_state.encode(buf, version, is_flexible)?;
        self.protocol_type.encode(buf, version, is_flexible)?;
        self.protocol_data.encode(buf, version, is_flexible)?;
        self.members.encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.authorized_operations
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribedGroup {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let error_code = <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let error_message = if (6) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let group_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_state = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let protocol_type = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let protocol_data = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let members =
            <Vec<DescribedGroupMember> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let authorized_operations = if (3) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        })
    }
}

impl KafkaSerialize for DescribedGroupMember {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.member_id.encode(buf, version, is_flexible)?;
        if (4) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        self.client_id.encode(buf, version, is_flexible)?;
        self.client_host.encode(buf, version, is_flexible)?;
        self.member_metadata.encode(buf, version, is_flexible)?;
        self.member_assignment.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribedGroupMember {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let member_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let group_instance_id = if (4) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let client_id = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let client_host = <String as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_metadata = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_assignment = <Vec<u8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            group_instance_id,
            client_id,
            client_host,
            member_metadata,
            member_assignment,
        })
    }
}
