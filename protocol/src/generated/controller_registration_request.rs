#![allow(unused_imports, unused_variables)]
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::serialization::{KafkaCodec, decode_unsigned_varint, encode_unsigned_varint};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVer, SerializationError};

// -------------------------------------------------------
// ControllerRegistrationRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControllerRegistrationRequest {
    /// The ID of the controller to register.
    pub controller_id: i32,
    /// The controller incarnation ID, which is unique to each process run.
    pub incarnation_id: [u8; 16],
    /// Set if the required configurations for ZK migration are present.
    pub zk_migration_ready: bool,
    /// The listeners of this controller.
    pub listeners: Vec<Listener>,
    /// The features on this controller.
    pub features: Vec<Feature>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Feature {
    /// The feature name.
    pub name: String,
    /// The minimum supported feature level.
    pub min_supported_version: i16,
    /// The maximum supported feature level.
    pub max_supported_version: i16,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    pub name: String,
    /// The hostname.
    pub host: String,
    /// The port.
    pub port: u16,
    /// The security protocol.
    pub security_protocol: i16,
}

impl ApiRequest for ControllerRegistrationRequest {
    type Response = crate::generated::ControllerRegistrationResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(70)
    }
    fn get_min_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_max_supported_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn get_min_flexible_version() -> ApiVer {
        ApiVer::new(0)
    }
    fn serialize(&self, version: ApiVer, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            0 <= version.0 && version.0 <= 0,
            "version {} is not supported by {} (supported: 0-0)",
            version.0,
            stringify!(Self)
        );
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        self.controller_id.encode(buf, version, is_flexible)?;
        self.incarnation_id.encode(buf, version, is_flexible)?;
        self.zk_migration_ready.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.features.encode(buf, version, is_flexible)?;
        if is_flexible {
            encode_unsigned_varint(0u64, buf);
        }
        Ok(())
    }
    fn deserialize(version: ApiVer, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let is_flexible = version.0 >= Self::get_min_flexible_version().0;
        let controller_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let zk_migration_ready = KafkaCodec::decode(buf, version, is_flexible)?;
        let listeners = KafkaCodec::decode(buf, version, is_flexible)?;
        let features = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            controller_id,
            incarnation_id,
            zk_migration_ready,
            listeners,
            features,
        })
    }
}
impl KafkaCodec for ControllerRegistrationRequest {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.controller_id.encode(buf, version, is_flexible)?;
        self.incarnation_id.encode(buf, version, is_flexible)?;
        self.zk_migration_ready.encode(buf, version, is_flexible)?;
        self.listeners.encode(buf, version, is_flexible)?;
        self.features.encode(buf, version, is_flexible)?;
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
        let controller_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let incarnation_id = KafkaCodec::decode(buf, version, is_flexible)?;
        let zk_migration_ready = KafkaCodec::decode(buf, version, is_flexible)?;
        let listeners = KafkaCodec::decode(buf, version, is_flexible)?;
        let features = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            controller_id,
            incarnation_id,
            zk_migration_ready,
            listeners,
            features,
        })
    }
}

impl KafkaCodec for Feature {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.min_supported_version
            .encode(buf, version, is_flexible)?;
        self.max_supported_version
            .encode(buf, version, is_flexible)?;
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let min_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        let max_supported_version = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            min_supported_version,
            max_supported_version,
        })
    }
}

impl KafkaCodec for Listener {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        version: ApiVer,
        is_flexible: bool,
    ) -> Result<(), SerializationError> {
        self.name.encode(buf, version, is_flexible)?;
        self.host.encode(buf, version, is_flexible)?;
        self.port.encode(buf, version, is_flexible)?;
        self.security_protocol.encode(buf, version, is_flexible)?;
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
        let name = KafkaCodec::decode(buf, version, is_flexible)?;
        let host = KafkaCodec::decode(buf, version, is_flexible)?;
        let port = KafkaCodec::decode(buf, version, is_flexible)?;
        let security_protocol = KafkaCodec::decode(buf, version, is_flexible)?;
        if is_flexible {
            let (_tag_count, _) = decode_unsigned_varint(buf)?;
        }
        Ok(Self {
            name,
            host,
            port,
            security_protocol,
        })
    }
}
