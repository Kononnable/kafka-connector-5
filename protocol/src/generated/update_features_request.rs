#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;

// -------------------------------------------------------
// UpdateFeaturesRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UpdateFeaturesRequest {
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// The list of updates to finalized features.
    /// IndexMap key `Feature` (string): The name of the finalized feature to be updated.
    pub feature_updates: IndexMap<String, FeatureUpdateKey>,
    /// True if we should validate the request, but not perform the upgrade or downgrade.
    /// Available in version 1+.
    pub validate_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FeatureUpdateKey {
    /// The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature.
    pub max_version_level: i16,
    /// DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level.
    /// Available in version 0.
    pub allow_downgrade: bool,
    /// Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy).
    /// Available in version 1+.
    pub upgrade_type: i8,
}

impl ApiRequest for UpdateFeaturesRequest {
    type Response = crate::generated::UpdateFeaturesResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(57)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(2)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 2,
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.feature_updates.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.validate_only.encode(buf, version, is_flexible)?;
        } else if self.validate_only {
            return Err(SerializationError::FieldNotAvailable {
                field: "ValidateOnly",
                version,
                api_name: "UpdateFeaturesRequest",
            });
        }
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let feature_updates = KafkaCodec::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            feature_updates,
            validate_only,
        })
    }
}
impl KafkaCodec for UpdateFeaturesRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.timeout_ms.encode(buf, version, is_flexible)?;
        self.feature_updates.encode(buf, version, is_flexible)?;
        if 1 <= version.0 {
            self.validate_only.encode(buf, version, is_flexible)?;
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
        let timeout_ms = KafkaCodec::decode(buf, version, is_flexible)?;
        let feature_updates = KafkaCodec::decode(buf, version, is_flexible)?;
        let validate_only = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            timeout_ms,
            feature_updates,
            validate_only,
        })
    }
}

impl KafkaCodec for FeatureUpdateKey {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.max_version_level.encode(buf, version, is_flexible)?;
        if version.0 == 0 {
            self.allow_downgrade.encode(buf, version, is_flexible)?;
        }
        if 1 <= version.0 {
            self.upgrade_type.encode(buf, version, is_flexible)?;
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
        let max_version_level = KafkaCodec::decode(buf, version, is_flexible)?;
        let allow_downgrade = if version.0 == 0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let upgrade_type = if 1 <= version.0 {
            KafkaCodec::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            max_version_level,
            allow_downgrade,
            upgrade_type,
        })
    }
}
