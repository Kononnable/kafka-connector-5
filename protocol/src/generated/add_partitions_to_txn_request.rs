#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{
    KafkaDeserialize, KafkaSerialize, decode_unsigned_varint, encode_unsigned_varint,
};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// AddPartitionsToTxnRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnRequest {
    /// List of transactions to add partitions to.
    /// Available in version 4+.
    pub transactions: Vec<AddPartitionsToTxnTransaction>,
    /// The transactional id corresponding to the transaction.
    /// Available in version 0-3.
    pub v3_and_below_transactional_id: String,
    /// Current producer id in use by the transactional id.
    /// Available in version 0-3.
    pub v3_and_below_producer_id: i64,
    /// Current epoch associated with the producer id.
    /// Available in version 0-3.
    pub v3_and_below_producer_epoch: i16,
    /// The partitions to add to the transaction.
    /// Available in version 0-3.
    pub v3_and_below_topics: Vec<AddPartitionsToTxnTopic>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnTopic {
    /// The name of the topic.
    pub name: String,
    /// The partition indexes to add to the transaction.
    pub partitions: Vec<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AddPartitionsToTxnTransaction {
    /// The transactional id corresponding to the transaction.
    /// Available in version 4+.
    pub transactional_id: String,
    /// Current producer id in use by the transactional id.
    /// Available in version 4+.
    pub producer_id: i64,
    /// Current epoch associated with the producer id.
    /// Available in version 4+.
    pub producer_epoch: i16,
    /// Boolean to signify if we want to check if the partition is in the transaction rather than add it.
    /// Available in version 4+.
    pub verify_only: bool,
    /// The partitions to add to the transaction.
    /// Available in version 4+.
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

impl ApiRequest for AddPartitionsToTxnRequest {
    type Response = crate::generated::AddPartitionsToTxnResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(24)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(5)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(3)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 5,
            "version {} is not supported by {} (supported: 0-5)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if 4 <= version.0 {
            self.transactions.encode(buf, version, is_flexible)?;
        } else if !self.transactions.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Transactions' is not available in this version",
            ));
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_transactional_id
                .encode(buf, version, is_flexible)?;
        } else if !self.v3_and_below_transactional_id.is_empty() {
            return Err(SerializationError::Encode(
                "field 'V3AndBelowTransactionalId' is not available in this version",
            ));
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_producer_id
                .encode(buf, version, is_flexible)?;
        } else if self.v3_and_below_producer_id != 0 {
            return Err(SerializationError::Encode(
                "field 'V3AndBelowProducerId' is not available in this version",
            ));
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_producer_epoch
                .encode(buf, version, is_flexible)?;
        } else if self.v3_and_below_producer_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'V3AndBelowProducerEpoch' is not available in this version",
            ));
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_topics.encode(buf, version, is_flexible)?;
        } else if !self.v3_and_below_topics.is_empty() {
            return Err(SerializationError::Encode(
                "field 'V3AndBelowTopics' is not available in this version",
            ));
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactions = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_transactional_id = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_producer_id = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_producer_epoch = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_topics = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactions,
            v3_and_below_transactional_id,
            v3_and_below_producer_id,
            v3_and_below_producer_epoch,
            v3_and_below_topics,
        })
    }
}
impl KafkaSerialize for AddPartitionsToTxnRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 4 <= version.0 {
            self.transactions.encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_transactional_id
                .encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_producer_id
                .encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_producer_epoch
                .encode(buf, version, is_flexible)?;
        }
        if 0 <= version.0 && version.0 <= 3 {
            self.v3_and_below_topics.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let transactions = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_transactional_id = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_producer_id = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_producer_epoch = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let v3_and_below_topics = if 0 <= version.0 && version.0 <= 3 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactions,
            v3_and_below_transactional_id,
            v3_and_below_producer_id,
            v3_and_below_producer_epoch,
            v3_and_below_topics,
        })
    }
}

impl KafkaSerialize for AddPartitionsToTxnTopic {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.partitions.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnTopic {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let name = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let partitions = KafkaDeserialize::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self { name, partitions })
    }
}

impl KafkaSerialize for AddPartitionsToTxnTransaction {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        if 4 <= version.0 {
            self.transactional_id.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.producer_epoch.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.verify_only.encode(buf, version, is_flexible)?;
        }
        if 4 <= version.0 {
            self.topics.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for AddPartitionsToTxnTransaction {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let transactional_id = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_id = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_epoch = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let verify_only = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let topics = if 4 <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            verify_only,
            topics,
        })
    }
}
