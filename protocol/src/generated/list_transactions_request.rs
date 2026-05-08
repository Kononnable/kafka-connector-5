#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListTransactionsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListTransactionsRequest {
    /// The transaction states to filter by: if empty, all transactions are returned; if non-empty, then only transactions matching one of the filtered states will be returned.
    pub state_filters: Vec<String>,
    /// The producerIds to filter by: if empty, all transactions will be returned; if non-empty, only transactions which match one of the filtered producerIds will be returned.
    pub producer_id_filters: Vec<i64>,
    /// Duration (in millis) to filter by: if < 0, all transactions will be returned; otherwise, only transactions running longer than this duration will be returned.
    /// Available in version 1+.
    pub duration_filter: i64,
    /// The transactional ID regular expression pattern to filter by: if it is empty or null, all transactions are returned; Otherwise then only the transactions matching the given regular expression will be returned.
    /// Available in version 2+.
    pub transactional_id_pattern: Option<String>,
}

impl ApiRequest for ListTransactionsRequest {
    type Response = crate::generated::ListTransactionsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(66)
    }
    fn get_min_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn get_max_supported_version() -> ApiVersionTrait {
        ApiVersionTrait::new(2)
    }
    fn get_min_flexible_version() -> ApiVersionTrait {
        ApiVersionTrait::new(0)
    }
    fn serialize(
        &self,
        version: ApiVersionTrait,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.state_filters.encode(buf, version, is_flexible)?;
        self.producer_id_filters.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.duration_filter.encode(buf, version, is_flexible)?;
        } else if self.duration_filter != 0 {
            return Err(SerializationError::Encode(
                "field 'DurationFilter' is not available in this version",
            ));
        }
        if (2) <= version.0 {
            self.transactional_id_pattern
                .encode(buf, version, is_flexible)?;
        } else if self.transactional_id_pattern.is_some() {
            return Err(SerializationError::Encode(
                "field 'TransactionalIdPattern' is not available in this version",
            ));
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVersionTrait, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let state_filters = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id_filters = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let duration_filter = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let transactional_id_pattern = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        })
    }
}
impl KafkaSerialize for ListTransactionsRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.state_filters.encode(buf, version, is_flexible)?;
        self.producer_id_filters.encode(buf, version, is_flexible)?;
        if (1) <= version.0 {
            self.duration_filter.encode(buf, version, is_flexible)?;
        }
        if (2) <= version.0 {
            self.transactional_id_pattern
                .encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ListTransactionsRequest {
    fn decode<B: Buf>(
        buf: &mut B,
        version: ApiVersionTrait,
        is_flexible: bool,
    ) -> Result<Self, SerializationError> {
        let state_filters = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let producer_id_filters = KafkaDeserialize::decode(buf, version, is_flexible)?;
        let duration_filter = if (1) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let transactional_id_pattern = if (2) <= version.0 {
            KafkaDeserialize::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            state_filters,
            producer_id_filters,
            duration_filter,
            transactional_id_pattern,
        })
    }
}
