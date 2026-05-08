#![allow(unused_imports, unused_variables)]
use crate::protocol::serialization::{KafkaDeserialize, KafkaSerialize};
use crate::traits::{
    ApiKey, ApiRequest, ApiResponse, ApiVersion as ApiVersionTrait, SerializationError,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// -------------------------------------------------------
// DefaultPrincipalData
// -------------------------------------------------------
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DefaultPrincipalData {
    /// The principal type.
    pub r#type: String,
    /// The principal name.
    pub name: String,
    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    pub token_authenticated: bool,
}
