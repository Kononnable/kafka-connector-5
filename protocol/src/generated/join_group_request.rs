#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// JoinGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinGroupRequest {
    /// The group identifier.
    pub group_id: String,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    pub session_timeout_ms: i32,
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    /// Available in version 1+.
    pub rebalance_timeout_ms: i32,
    /// The member id assigned by the group coordinator.
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    /// Available in version 5+.
    pub group_instance_id: Option<String>,
    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: String,
    /// The list of protocols that the member supports.
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// The reason why the member (re-)joins the group.
    /// Available in version 8+.
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinGroupRequestProtocol {
    /// The protocol name.
    pub name: String,
    /// The protocol metadata.
    pub metadata: Vec<u8>,
}

impl ApiRequest for JoinGroupRequest {
    type Response = crate::generated::JoinGroupResponse;
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
        self.group_id.encode(buf, version, is_flexible)?;
        self.session_timeout_ms.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.rebalance_timeout_ms
                .encode(buf, version, is_flexible)?;
        } else if self.rebalance_timeout_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'RebalanceTimeoutMs' is not available in this version",
            ));
        }
        self.member_id.encode(buf, version, is_flexible)?;
        if (5) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        } else if self.group_instance_id.is_some() {
            return Err(SerializationError::Encode(
                "field 'GroupInstanceId' is not available in this version",
            ));
        }
        self.protocol_type.encode(buf, version, is_flexible)?;
        self.protocols.encode(buf, version, is_flexible)?;
        if (8) <= version.0 {
            self.reason.encode(buf, version, is_flexible)?;
        } else if self.reason.is_some() {
            return Err(SerializationError::Encode(
                "field 'Reason' is not available in this version",
            ));
        }
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
        let session_timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rebalance_timeout_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let protocols = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let reason = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        })
    }
}
impl KafkaSerialize for JoinGroupRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.session_timeout_ms.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.rebalance_timeout_ms
                .encode(buf, version, is_flexible)?;
        }
        self.member_id.encode(buf, version, is_flexible)?;
        if (5) <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        self.protocol_type.encode(buf, version, is_flexible)?;
        self.protocols.encode(buf, version, is_flexible)?;
        if (8) <= version.0 {
            self.reason.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let group_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let session_timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let rebalance_timeout_ms = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let member_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let group_instance_id = if (5) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let protocol_type = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let protocols = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let reason = if (8) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        })
    }
}

impl KafkaSerialize for JoinGroupRequestProtocol {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.metadata.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for JoinGroupRequestProtocol {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let metadata = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, metadata })
    }
}
