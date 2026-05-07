#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// UpdateFeaturesRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateFeaturesRequest {
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// The list of updates to finalized features.
    pub feature_updates: Vec<FeatureUpdateKey>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FeatureUpdateKey {
    /// The name of the finalized feature to be updated.
    pub feature: String,
    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
    pub max_version_level: i16,
    /// When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
    pub allow_downgrade: bool,
}

impl ApiRequest for UpdateFeaturesRequest {
    type Response = crate::generated::UpdateFeaturesResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(57)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (0),
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        self.timeout_ms
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode timeoutMs"))?;
        self.feature_updates
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode FeatureUpdates"))?;
        Ok(())
    }
    fn deserialize(
        version: crate::traits::ApiVersion,
        buf: &mut Bytes,
    ) -> Result<Self, SerializationError> {
        let timeout_ms = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode timeoutMs"))?;
        let feature_updates = <Vec<FeatureUpdateKey> as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode FeatureUpdates"))?;
        Ok(Self {
            timeout_ms,
            feature_updates,
        })
    }
}
impl KafkaSerialize for UpdateFeaturesRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.timeout_ms
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode timeoutMs".into(),
            })?;
        self.feature_updates
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode FeatureUpdates".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for UpdateFeaturesRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let timeout_ms =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode timeoutMs".into(),
            })?;
        let feature_updates =
            <Vec<FeatureUpdateKey> as KafkaDeserialize>::decode(buf).map_err(|_| {
                DecodeError::Protocol {
                    message: "failed to decode FeatureUpdates".into(),
                }
            })?;
        Ok(Self {
            timeout_ms,
            feature_updates,
        })
    }
}

impl KafkaSerialize for FeatureUpdateKey {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.feature
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode Feature".into(),
            })?;
        self.max_version_level
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode MaxVersionLevel".into(),
            })?;
        self.allow_downgrade
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode AllowDowngrade".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for FeatureUpdateKey {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let feature =
            <String as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode Feature".into(),
            })?;
        let max_version_level =
            <i16 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode MaxVersionLevel".into(),
            })?;
        let allow_downgrade =
            <bool as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode AllowDowngrade".into(),
            })?;
        Ok(Self {
            feature,
            max_version_level,
            allow_downgrade,
        })
    }
}
