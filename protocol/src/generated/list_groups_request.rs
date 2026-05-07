#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ListGroupsRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ListGroupsRequest {
    /// The states of the groups we want to list. If empty, all groups are returned with their state.
    /// Available in version 4+.
    pub states_filter: Vec<String>,
    /// The types of the groups we want to list. If empty, all groups are returned with their type.
    /// Available in version 5+.
    pub types_filter: Vec<String>,
}

impl ApiRequest for ListGroupsRequest {
    type Response = crate::generated::ListGroupsResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(16)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(5)
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
        let is_flexible = (3) <= version.0;
        if (4) <= version.0 {
            self.states_filter
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode StatesFilter"))?;
        }
        if (5) <= version.0 {
            self.types_filter
                .encode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Encode("failed to encode TypesFilter"))?;
        }
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
        let is_flexible = (3) <= version.0;
        let states_filter = if (4) <= version.0 {
            <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode StatesFilter"))?
        } else {
            Default::default()
        };
        let types_filter = if (5) <= version.0 {
            <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
                .map_err(|_| SerializationError::Decode("failed to decode TypesFilter"))?
        } else {
            Default::default()
        };
        Ok(Self {
            states_filter,
            types_filter,
        })
    }
}
impl KafkaSerialize for ListGroupsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.states_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StatesFilter".into(),
            })?;
        self.types_filter
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TypesFilter".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.states_filter
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode StatesFilter".into(),
            })?;
        self.types_filter
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode TypesFilter".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for ListGroupsRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let states_filter =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode StatesFilter".into(),
            })?;
        let types_filter =
            <Vec<String> as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode TypesFilter".into(),
            })?;
        Ok(Self {
            states_filter,
            types_filter,
        })
    }
    fn decode_flexible<B: Buf>(buf: &mut B, is_flexible: bool) -> Result<Self, DecodeError> {
        let states_filter = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode StatesFilter".into(),
            })?;
        let types_filter = <Vec<String> as KafkaDeserialize>::decode_flexible(buf, is_flexible)
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode TypesFilter".into(),
            })?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            states_filter,
            types_filter,
        })
    }
}
