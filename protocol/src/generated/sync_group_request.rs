#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// SyncGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SyncGroupRequest {
    /// The unique group identifier.
    pub group_id: String,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID assigned by the group.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
    /// The group protocol type.
    /// Available in version 5+.
    pub protocol_type: Option<String>,
    /// The group protocol name.
    /// Available in version 5+.
    pub protocol_name: Option<String>,
    /// Each assignment.
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    pub member_id: String,
    /// The member assignment.
    pub assignment: Vec<u8>,
}

impl ApiRequest for SyncGroupRequest {
    type Response = crate::generated::SyncGroupResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(14)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(4)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (5),
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        self.generation_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        } else if self.group_instance_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'GroupInstanceId' is not available in this version",
            ));
        }
        if (5) <= version.0 {
            self.protocol_type.encode(buf, version, is_flexible)?;
        } else if self.protocol_type.is_some() {
            return Err(SerializationError::Encode(
                "field 'ProtocolType' is not available in this version",
            ));
        }
        if (5) <= version.0 {
            self.protocol_name.encode(buf, version, is_flexible)?;
        } else if self.protocol_name.is_some() {
            return Err(SerializationError::Encode(
                "field 'ProtocolName' is not available in this version",
            ));
        }
        self.assignments.encode(buf, version, is_flexible)?;
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
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_type = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_name = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let assignments = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
}
impl KafkaSerialize for SyncGroupRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.generation_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        if (5) <= version.0 {
            self.protocol_type.encode(buf, version, is_flexible)?;
        }
        if (5) <= version.0 {
            self.protocol_name.encode(buf, version, is_flexible)?;
        }
        self.assignments.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SyncGroupRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_type = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_name = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let assignments = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        })
    }
}

impl KafkaSerialize for SyncGroupRequestAssignment {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.member_id.encode(buf, version, is_flexible)?;
        self.assignment.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for SyncGroupRequestAssignment {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let assignment = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            member_id,
            assignment,
        })
    }
}
