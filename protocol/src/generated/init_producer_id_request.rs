#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// InitProducerIdRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InitProducerIdRequest {
    /// The transactional id, or null if the producer is not transactional.
    pub transactional_id: Option<String>,
    /// The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.
    pub transaction_timeout_ms: i32,
    /// The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.
    /// Available in version 3+.
    pub producer_id: i64,
    /// The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.
    /// Available in version 3+.
    pub producer_epoch: i16,
    /// True if the client wants to enable two-phase commit (2PC) protocol for transactions.
    /// Available in version 6+.
    pub enable2_pc: bool,
    /// True if the client wants to keep the currently ongoing transaction instead of aborting it.
    /// Available in version 6+.
    pub keep_prepared_txn: bool,
}

impl ApiRequest for InitProducerIdRequest {
    type Response = crate::generated::InitProducerIdResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(22)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(6)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.transaction_timeout_ms
            .encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        } else if self.producer_id != 0 {
            return Err(SerializationError::Encode(
                "field 'ProducerId' is not available in this version",
            ));
        }
        if (3) <= version.0 {
            self.producer_epoch.encode(buf, version, is_flexible)?;
        } else if self.producer_epoch != 0 {
            return Err(SerializationError::Encode(
                "field 'ProducerEpoch' is not available in this version",
            ));
        }
        if (6) <= version.0 {
            self.enable2_pc.encode(buf, version, is_flexible)?;
        } else if self.enable2_pc {
            return Err(SerializationError::Encode(
                "field 'Enable2Pc' is not available in this version",
            ));
        }
        if (6) <= version.0 {
            self.keep_prepared_txn.encode(buf, version, is_flexible)?;
        } else if self.keep_prepared_txn {
            return Err(SerializationError::Encode(
                "field 'KeepPreparedTxn' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let transactional_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_epoch = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let enable2_pc = if (6) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let keep_prepared_txn = if (6) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
        })
    }
}
impl KafkaSerialize for InitProducerIdRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.transactional_id.encode(buf, version, is_flexible)?;
        self.transaction_timeout_ms
            .encode(buf, version, is_flexible)?;
        if (3) <= version.0 {
            self.producer_id.encode(buf, version, is_flexible)?;
        }
        if (3) <= version.0 {
            self.producer_epoch.encode(buf, version, is_flexible)?;
        }
        if (6) <= version.0 {
            self.enable2_pc.encode(buf, version, is_flexible)?;
        }
        if (6) <= version.0 {
            self.keep_prepared_txn.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for InitProducerIdRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let transactional_id = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let transaction_timeout_ms = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let producer_epoch = if (3) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let enable2_pc = if (6) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let keep_prepared_txn = if (6) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            transactional_id,
            transaction_timeout_ms,
            producer_id,
            producer_epoch,
            enable2_pc,
            keep_prepared_txn,
        })
    }
}
