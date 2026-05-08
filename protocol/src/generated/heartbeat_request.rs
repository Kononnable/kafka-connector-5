#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// HeartbeatRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HeartbeatRequest {
    /// The group id.
    pub group_id: String,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 3+.
    pub group_instance_id: Option<String>,
}

impl ApiRequest for HeartbeatRequest {
    type Response = crate::generated::HeartbeatResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(12)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(4)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(4)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (4),
            "version {} is not supported by {} (supported: 0-4)",
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
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        })
    }
}
impl KafkaSerialize for HeartbeatRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.generation_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for HeartbeatRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let generation_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        })
    }
}
