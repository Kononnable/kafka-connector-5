#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// JoinGroupRequest
// -------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
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
    /// IndexMap key `Name` (string): The protocol name.
    pub protocols: IndexMap<String, JoinGroupRequestProtocol>,
    /// The reason why the member (re-)joins the group.
    /// Available in version 8+.
    pub reason: Option<String>,
}
impl Default for JoinGroupRequest {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            session_timeout_ms: 0,
            rebalance_timeout_ms: -1,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: String::new(),
            protocols: IndexMap::new(),
            reason: None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinGroupRequestProtocol {
    /// The protocol metadata.
    pub metadata: Vec<u8>,
}

impl ApiRequest for JoinGroupRequest {
    type Response = crate::generated::JoinGroupResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(11)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(9)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 9,
            "version {} is not supported by {} (supported: 0-9)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        self.session_timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.rebalance_timeout_ms
                .encode(buf, version, is_flexible)?;
        } else if self.rebalance_timeout_ms != -1 {
            return Err(SerializationError::FieldNotAvailable {
                field: "RebalanceTimeoutMs",
                version,
                api_name: "JoinGroupRequest",
            });
        }
        self.member_id.encode(buf, version, is_flexible)?;
        if 5 <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        } else if self.group_instance_id.is_some() {
            return Err(SerializationError::FieldNotAvailable {
                field: "GroupInstanceId",
                version,
                api_name: "JoinGroupRequest",
            });
        }
        self.protocol_type.encode(buf, version, is_flexible)?;
        self.protocols.encode(buf, version, is_flexible)?;
        if 8 <= version.0 {
            self.reason.encode(buf, version, is_flexible)?;
        } else if self.reason.is_some() {
            return Err(SerializationError::FieldNotAvailable {
                field: "Reason",
                version,
                api_name: "JoinGroupRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let session_timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let rebalance_timeout_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let member_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_instance_id = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let protocol_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let protocols = KafkaCodec::decode(buf, version, is_flexible)?;
        let reason = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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
impl KafkaCodec for JoinGroupRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.session_timeout_ms.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.rebalance_timeout_ms
                .encode(buf, version, is_flexible)?;
        }
        self.member_id.encode(buf, version, is_flexible)?;
        if 5 <= version.0 {
            self.group_instance_id.encode(buf, version, is_flexible)?;
        }
        self.protocol_type.encode(buf, version, is_flexible)?;
        self.protocols.encode(buf, version, is_flexible)?;
        if 8 <= version.0 {
            self.reason.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let group_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let session_timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let rebalance_timeout_ms = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            -1
        };
        let member_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let group_instance_id = if 5 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        let protocol_type = KafkaCodec::decode(buf, version, is_flexible)?;
        let protocols = KafkaCodec::decode(buf, version, is_flexible)?;
        let reason = if 8 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            None
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
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

impl KafkaCodec for JoinGroupRequestProtocol {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.metadata.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }

    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let metadata = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { metadata })
    }
}
