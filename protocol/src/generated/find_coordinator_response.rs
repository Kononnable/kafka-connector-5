#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// FindCoordinatorResponse
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    /// Available in version 1+.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    /// Available in version 0-3.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 1-3.
    pub error_message: Option<String>,
    /// The node id.
    /// Available in version 0-3.
    pub node_id: i32,
    /// The host name.
    /// Available in version 0-3.
    pub host: String,
    /// The port.
    /// Available in version 0-3.
    pub port: i32,
    /// Each coordinator result in the response.
    /// Available in version 4+.
    pub coordinators: Vec<Coordinator>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Coordinator {
    /// The coordinator key.
    /// Available in version 4+.
    pub key: String,
    /// The node id.
    /// Available in version 4+.
    pub node_id: i32,
    /// The host name.
    /// Available in version 4+.
    pub host: String,
    /// The port.
    /// Available in version 4+.
    pub port: i32,
    /// The error code, or 0 if there was no error.
    /// Available in version 4+.
    pub error_code: i16,
    /// The error message, or null if there was no error.
    /// Available in version 4+.
    pub error_message: Option<String>,
}

impl ApiResponse for FindCoordinatorResponse {
    type Request = crate::generated::FindCoordinatorRequest;
    fn get_api_key() -> ApiKey {
        ApiKey::new(10)
    }
    fn get_min_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(0)
    }
    fn get_max_supported_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(6)
    }
    fn get_min_flexible_version() -> crate::traits::ApiVersion {
        crate::traits::ApiVersion::new(3)
    }
    fn serialize(
        &self,
        version: crate::traits::ApiVersion,
        buf: &mut BytesMut,
    ) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (6),
            "version {} is not supported by {} (supported: 0-6)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        } else if self.throttle_time_ms != 0 {
            return Err(SerializationError::Encode(
                "field 'ThrottleTimeMs' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.error_code.encode(buf, version, is_flexible)?;
        } else if self.error_code != 0 {
            return Err(SerializationError::Encode(
                "field 'ErrorCode' is not available in this version",
            ));
        }
        if (1) <= version.0 && version.0 <= (3) {
            self.error_message.encode(buf, version, is_flexible)?;
        } else if self.error_message.is_some() {
            return Err(SerializationError::Encode(
                "field 'ErrorMessage' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.node_id.encode(buf, version, is_flexible)?;
        } else if self.node_id != 0 {
            return Err(SerializationError::Encode(
                "field 'NodeId' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.host.encode(buf, version, is_flexible)?;
        } else if !self.host.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Host' is not available in this version",
            ));
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.port.encode(buf, version, is_flexible)?;
        } else if self.port != 0 {
            return Err(SerializationError::Encode(
                "field 'Port' is not available in this version",
            ));
        }
        if (4) <= version.0 {
            self.coordinators.encode(buf, version, is_flexible)?;
        } else if !self.coordinators.is_empty() {
            return Err(SerializationError::Encode(
                "field 'Coordinators' is not available in this version",
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
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (0) <= version.0 && version.0 <= (3) {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_message = if (1) <= version.0 && version.0 <= (3) {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let node_id = if (0) <= version.0 && version.0 <= (3) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (0) <= version.0 && version.0 <= (3) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (0) <= version.0 && version.0 <= (3) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let coordinators = if (4) <= version.0 {
            <Vec<Coordinator> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
            coordinators,
        })
    }
}
impl KafkaSerialize for FindCoordinatorResponse {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (1) <= version.0 {
            self.throttle_time_ms.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if (1) <= version.0 && version.0 <= (3) {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.node_id.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (0) <= version.0 && version.0 <= (3) {
            self.port.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.coordinators.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for FindCoordinatorResponse {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let throttle_time_ms = if (1) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (0) <= version.0 && version.0 <= (3) {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_message = if (1) <= version.0 && version.0 <= (3) {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let node_id = if (0) <= version.0 && version.0 <= (3) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (0) <= version.0 && version.0 <= (3) {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (0) <= version.0 && version.0 <= (3) {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let coordinators = if (4) <= version.0 {
            <Vec<Coordinator> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
            coordinators,
        })
    }
}

impl KafkaSerialize for Coordinator {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<(), crate::traits::SerializationError> {
        if (4) <= version.0 {
            self.key.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.node_id.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.host.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.port.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.error_code.encode(buf, version, is_flexible)?;
        }
        if (4) <= version.0 {
            self.error_message.encode(buf, version, is_flexible)?;
        }
        if is_flexible {
            crate::protocol::serialization::encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
}

impl KafkaDeserialize for Coordinator {
    fn decode<B: Buf>(
        buf: &mut B,
        version: crate::traits::ApiVersion,
        is_flexible: bool,
    ) -> Result<Self, crate::traits::SerializationError> {
        let key = if (4) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let node_id = if (4) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let host = if (4) <= version.0 {
            <String as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let port = if (4) <= version.0 {
            <i32 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_code = if (4) <= version.0 {
            <i16 as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        let error_message = if (4) <= version.0 {
            <Option<String> as KafkaDeserialize>::decode(buf, version, is_flexible)?
        } else {
            Default::default()
        };
        if is_flexible {
            let (_tag_count, _) = crate::protocol::serialization::decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            key,
            node_id,
            host,
            port,
            error_code,
            error_message,
        })
    }
}
