#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{DecodeError, EncodeError, KafkaDeserialize, KafkaSerialize};
use crate::traits::{ApiKey, ApiRequest, ApiResponse, ApiVersion, SerializationError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// ControlledShutdownRequest
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ControlledShutdownRequest {
    /// The id of the broker for which controlled shutdown has been requested.
    pub broker_id: i32,
    /// The broker epoch.
    /// Available in version 2+.
    pub broker_epoch: i64,
}

impl ApiRequest for ControlledShutdownRequest {
    type Response = crate::generated::ControlledShutdownResponse;
    fn get_api_key() -> ApiKey {
        ApiKey::new(7)
    }
    fn get_min_supported_version() -> ApiVersion {
        ApiVersion::new(0)
    }
    fn get_max_supported_version() -> ApiVersion {
        ApiVersion::new(2)
    }
    fn serialize(&self, version: ApiVersion, buf: &mut BytesMut) -> Result<(), SerializationError> {
        assert!(
            (0) <= version.0 && version.0 <= (2),
            "version {} is not supported by {} (supported: 0-2)",
            version.0,
            stringify!(Self)
        );
        self.broker_id
            .encode(buf)
            .map_err(|_| SerializationError::Encode("failed to encode BrokerId"))?;
        if (2) <= version.0 {
            self.broker_epoch
                .encode(buf)
                .map_err(|_| SerializationError::Encode("failed to encode BrokerEpoch"))?;
        }
        Ok(())
    }
    fn deserialize(version: ApiVersion, buf: &mut Bytes) -> Result<Self, SerializationError> {
        let broker_id = <i32 as KafkaDeserialize>::decode(buf)
            .map_err(|_| SerializationError::Decode("failed to decode BrokerId"))?;
        let broker_epoch = if (2) <= version.0 {
            <i64 as KafkaDeserialize>::decode(buf)
                .map_err(|_| SerializationError::Decode("failed to decode BrokerEpoch"))?
        } else {
            Default::default()
        };
        Ok(Self {
            broker_id,
            broker_epoch,
        })
    }
}
impl KafkaSerialize for ControlledShutdownRequest {
    fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), EncodeError> {
        self.broker_id
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerId".into(),
            })?;
        self.broker_epoch
            .encode(buf)
            .map_err(|_| EncodeError::ValueTooLarge {
                message: "failed to encode BrokerEpoch".into(),
            })?;
        Ok(())
    }
}

impl KafkaDeserialize for ControlledShutdownRequest {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self, DecodeError> {
        let broker_id =
            <i32 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerId".into(),
            })?;
        let broker_epoch =
            <i64 as KafkaDeserialize>::decode(buf).map_err(|_| DecodeError::Protocol {
                message: "failed to decode BrokerEpoch".into(),
            })?;
        Ok(Self {
            broker_id,
            broker_epoch,
        })
    }
}
