#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ShareAcknowledgeRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShareAcknowledgeRequest {
    /// The group identifier.
    pub group_id: Option<String>,
    /// The member ID.
    pub member_id: Option<String>,
    /// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
    pub share_session_epoch: i32,
    /// The topics containing records to acknowledge.
    pub topics: Vec<AcknowledgeTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcknowledgePartition {
    /// The partition index.
    pub partition_index: i32,
    /// Record batches to acknowledge.
    pub acknowledgement_batches: Vec<AcknowledgementBatch>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcknowledgeTopic {
    /// The unique topic ID.
    pub topic_id: [u8; 16],
    /// The partitions containing records to acknowledge.
    pub partitions: Vec<AcknowledgePartition>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AcknowledgementBatch {
    /// First offset of batch of records to acknowledge.
    pub first_offset: i64,
    /// Last offset (inclusive) of batch of records to acknowledge.
    pub last_offset: i64,
    /// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject.
    pub acknowledge_types: Vec<i8>,
}

impl ApiRequest for ShareAcknowledgeRequest {
    type Response = crate::generated::ShareAcknowledgeResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(79)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (1),
            "version {} is not supported by {} (supported: 1-1)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.group_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        self.share_session_epoch.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
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
        let group_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let share_session_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics =
            <Vec<AcknowledgeTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            share_session_epoch,
            topics,
        })
    }
}
impl KafkaSerialize for ShareAcknowledgeRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.group_id.encode(buf, version, is_flexible)?;
        self.member_id.encode(buf, version, is_flexible)?;
        self.share_session_epoch.encode(buf, version, is_flexible)?;
        self.topics.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ShareAcknowledgeRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let group_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let member_id = <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let share_session_epoch = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let topics =
            <Vec<AcknowledgeTopic> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            group_id,
            member_id,
            share_session_epoch,
            topics,
        })
    }
}

impl KafkaSerialize for AcknowledgePartition {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.partition_index.encode(buf, version, is_flexible)?;
        self.acknowledgement_batches
            .encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AcknowledgePartition {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let partition_index = <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let acknowledgement_batches =
            <Vec<AcknowledgementBatch> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            partition_index,
            acknowledgement_batches,
        })
    }
}

impl KafkaSerialize for AcknowledgeTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.topic_id.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AcknowledgeTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let topic_id = <[u8; 16] as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let partitions =
            <Vec<AcknowledgePartition> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            topic_id,
            partitions,
        })
    }
}

impl KafkaSerialize for AcknowledgementBatch {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        self.first_offset.encode(buf, version, is_flexible)?;
        self.last_offset.encode(buf, version, is_flexible)?;
        self.acknowledge_types.encode(buf, version, is_flexible)?;
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AcknowledgementBatch {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let first_offset = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let last_offset = <i64 as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        let acknowledge_types = <Vec<i8> as KafkaDeserialize>::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            first_offset,
            last_offset,
            acknowledge_types,
        })
    }
}
