//! Kafka client library.
//!
//! Single background event-loop thread, handles (channel senders) for the public API.
//!
//! ```ignore
//! let client = KafkaCluster::new(KafkaOptions::default())?;
//!
//! let producer = client.producer(ProducerOptions::default())?;
//! producer.send(ProducerRecord::new("topic", "key", "value"))?;
//!
//! let consumer = client.consumer(ConsumerOptions::default())?;
//! consumer.subscribe(&["topic"])?;
//! let records = consumer.poll(Duration::from_millis(100))?;
//! ```

// ---------------------------------------------------------------------------
// Public API types
// ---------------------------------------------------------------------------

pub struct KafkaOptions {
    pub bootstrap_servers: Vec<String>,
    pub metadata_max_age_ms: u64,
    pub request_timeout_ms: u64,
    pub retry_backoff_ms: u64,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".into()],
            metadata_max_age_ms: 300_000,
            request_timeout_ms: 30_000,
            retry_backoff_ms: 100,
        }
    }
}

pub struct ProducerOptions {
    pub acks: AcksMode,
    pub compression: Compression,
    pub compression_level: Option<i32>,
    pub batch_size: usize,
    pub linger_ms: u64,
    pub max_request_size: usize,
    pub enable_idempotence: bool,
    pub max_in_flight_per_connection: u16,
    pub delivery_timeout_ms: u64,
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            acks: AcksMode::All,
            compression: Compression::None,
            compression_level: None,
            batch_size: 16_384,
            linger_ms: 5,
            max_request_size: 1_048_576,
            enable_idempotence: true,
            max_in_flight_per_connection: 5,
            delivery_timeout_ms: 120_000,
        }
    }
}

pub enum AcksMode {
    None,
    Leader,
    All,
}

pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

pub struct ConsumerOptions {
    pub group_id: Option<String>,
    pub group_protocol: GroupProtocol,
    pub auto_offset_reset: OffsetReset,
    pub enable_auto_commit: bool,
    pub auto_commit_interval_ms: u64,
    pub fetch_min_bytes: u32,
    pub fetch_max_bytes: u32,
    pub max_partition_fetch_bytes: u32,
    pub max_poll_records: u32,
    pub max_poll_interval_ms: u64,
    pub session_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub isolation_level: IsolationLevel,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            group_id: None,
            group_protocol: GroupProtocol::Classic,
            auto_offset_reset: OffsetReset::Latest,
            enable_auto_commit: true,
            auto_commit_interval_ms: 5_000,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52_428_800,
            max_partition_fetch_bytes: 1_048_576,
            max_poll_records: 500,
            max_poll_interval_ms: 300_000,
            session_timeout_ms: 45_000,
            heartbeat_interval_ms: 3_000,
            isolation_level: IsolationLevel::ReadUncommitted,
        }
    }
}

pub enum GroupProtocol {
    Classic,
    Consumer,
}

pub enum OffsetReset {
    Earliest,
    Latest,
    None,
}

pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl ProducerRecord {
    pub fn new(topic: impl Into<String>, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: Some(key.into()),
            value: Some(value.into()),
            headers: vec![],
        }
    }
}

pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp_ms: i64,
}

pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

pub enum Offset {
    Beginning,
    End,
    Offset(i64),
    Timestamp(i64),
}

// ---------------------------------------------------------------------------
// Client handles
// ---------------------------------------------------------------------------

pub struct KafkaCluster;

pub struct Producer;

pub struct Consumer;

impl KafkaCluster {
    pub fn new(_options: KafkaOptions) -> Result<Self, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn producer(&self, _options: ProducerOptions) -> Result<Producer, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn consumer(&self, _options: ConsumerOptions) -> Result<Consumer, Box<dyn std::error::Error>> {
        todo!()
    }
}

impl Producer {
    pub fn send(&self, _record: ProducerRecord) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn flush(&self, _timeout: std::time::Duration) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

impl Consumer {
    pub fn subscribe(&self, _topics: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn assign(&self, _partitions: &[TopicPartition]) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn poll(&self, _timeout: std::time::Duration) -> Result<Vec<ConsumerRecord>, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn commit_sync(&self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
