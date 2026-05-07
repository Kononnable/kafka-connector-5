#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DescribeDelegationTokenRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeDelegationTokenRequest {
    /// Each owner that we want to describe delegation tokens for, or null to describe all tokens.
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DescribeDelegationTokenOwner {
    /// The owner principal type.
    pub principal_type: String,
    /// The owner principal name.
    pub principal_name: String,
}

impl ApiRequest for DescribeDelegationTokenRequest {
    type Response = crate::generated::DescribeDelegationTokenResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(41)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(1)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (1) <= version.0 && version.0 <= (3),
            "version {} is not supported by {} (supported: 1-3)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = (2) <= version.0;
        self.owners
            .encode_flexible(buf, is_flexible)
            .map_err(|_| SerializationError::Encode("failed to encode Owners"))?;
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
        let is_flexible = (2) <= version.0;
        let owners =
            <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode_flexible(
                buf,
                version,
                is_flexible,
            )
            .map_err(|_| SerializationError::Decode("failed to decode Owners"))?;
        Ok(Self { owners })
    }
}
impl KafkaSerialize for DescribeDelegationTokenRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.owners
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Owners".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        if is_flexible {
            if let Some(ref __val) = self.owners {
                crate::protocol::serialization::encode_unsigned_varint(1u64, buf);
                __val
                    .encode_flexible(buf, true)
                    .map_err(|_| EncodeError::ValueTooLarge {
                        message: "failed to encode Owners".into(),
                    })?;
            } else {
                crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
            }
        } else {
            if let Some(ref __val) = self.owners {
                __val.encode(buf).map_err(|_| EncodeError::ValueTooLarge {
                    message: "failed to encode Owners".into(),
                })?;
            }
        }
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `Owners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let owners = <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode(buf)
            .map_err(|_| DecodeError::Protocol {
            message: "failed to decode Owners".into(),
        })?;
        Ok(Self { owners })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `Owners` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let owners = if is_flexible {
            <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode_flexible(
                buf, version, true,
            )
            .map_err(|_| DecodeError::Protocol {
                message: "failed to decode Owners".into(),
            })?
        } else {
            <Option<Vec<DescribeDelegationTokenOwner>> as KafkaDeserialize>::decode(buf).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode Owners".into(),
                },
            )?
        };
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self { owners })
    }
}

impl KafkaSerialize for DescribeDelegationTokenOwner {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.principal_type
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        Ok(())
    }
    fn encode_flexible<B: BufMut>(
        &self,
        buf: &mut B,
        is_flexible: bool,
    ) -> Result<(), EncodeError> {
        self.principal_type
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalType".into(),
            })?;
        self.principal_name
            .encode_flexible(buf, is_flexible)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode PrincipalName".into(),
            })?;
        if is_flexible {
            // Tagged fields (none yet)
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for DescribeDelegationTokenOwner {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] classic decode field `PrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_type =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalType".into(),
            })?;
        tracing::trace!(
            "  [{}] classic decode field `PrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_name =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode PrincipalName".into(),
            })?;
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
    fn decode_flexible<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, DecodeError> {
        tracing::trace!(
            "  [{}] decoding field `PrincipalType` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_type =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PrincipalType".into(),
                },
            )?;
        tracing::trace!(
            "  [{}] decoding field `PrincipalName` ({} bytes remaining)",
            stringify!(Self),
            buf.remaining()
        );
        let principal_name =
            <String as KafkaDeserialize>::decode_flexible(buf, version, is_flexible).map_err(
                |_| DecodeError::Protocol {
                    message: "failed to decode PrincipalName".into(),
                },
            )?;
        if is_flexible {
            // Tagged fields (skip)
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            principal_type,
            principal_name,
        })
    }
}
